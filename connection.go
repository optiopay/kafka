package kafka

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"math"
	"net"
	"sync"
	"time"

	"github.com/optiopay/kafka/proto"
)

// ErrClosed is returned as result of any request made using closed connection.
var ErrClosed = errors.New("closed")

// Low level abstraction over connection to Kafka.
type connection struct {
	rw     net.Conn
	stop   chan struct{}
	logger Logger

	mu          sync.Mutex
	respc       map[int32]chan []byte
	stopErr     error
	readTimeout time.Duration
	lastID      int32
	lastIDMutex sync.Mutex
}

// newTCPConnection returns new, initialized connection using plain text or error
func newTCPConnection(ctx context.Context, address string, timeout, readTimeout time.Duration) (*connection, error) {
	dialer := net.Dialer{
		Timeout:   timeout,
		KeepAlive: 30 * time.Second,
	}
	tcpConn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	c := prepareConnection(tcpConn, readTimeout)
	return c, nil
}

// newTLSConnection returns new, initialized connection using TLS or error
func newTLSConnection(ctx context.Context, address string, config *tls.Config, timeout, readTimeout time.Duration) (*connection, error) {
	dialer := net.Dialer{
		Timeout:   timeout,
		KeepAlive: 30 * time.Second,
	}
	tcpConn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, err
	}
	tlsConn := tls.Client(tcpConn, config)
	c := prepareConnection(tlsConn, readTimeout)
	return c, nil
}

// prepareConnection returns new, initialized connection and starts go routines
// for reading and ID generation.
func prepareConnection(conn net.Conn, readTimeout time.Duration) *connection {
	c := &connection{
		stop:        make(chan struct{}),
		rw:          conn,
		respc:       make(map[int32]chan []byte),
		logger:      &nullLogger{},
		readTimeout: readTimeout,
		lastID:      0,
	}
	go c.readRespLoop()
	return c
}

// nextID generates correlation IDs, making sure they are always in order
// and within the scope of request-response mapping array.
func (c *connection) nextID() int32 {
	c.lastIDMutex.Lock()
	defer c.lastIDMutex.Unlock()

	c.lastID++
	if c.lastID == math.MaxInt32 {
		c.lastID = 1
	}
	return c.lastID
}

// readRespLoop constantly reading response messages from the socket and after
// partial parsing, sends byte representation of the whole message to request
// sending process.
func (c *connection) readRespLoop() {
	defer func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		for _, cc := range c.respc {
			close(cc)
		}
		c.respc = make(map[int32]chan []byte)
	}()

	stop := func(closeStopChannel bool, err error) {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.stopErr == nil {
			c.stopErr = err
			if closeStopChannel {
				close(c.stop)
			}
		}
	}

	rd := bufio.NewReader(c.rw)
	for {
		if c.readTimeout > 0 {
			err := c.rw.SetReadDeadline(time.Now().Add(c.readTimeout))
			if err != nil {
				c.logger.Error("msg", "SetReadDeadline failed",
					"error", err)
			}
		}
		correlationID, b, err := proto.ReadResp(rd)
		if err != nil {
			stop(true, err)
			return
		}

		c.mu.Lock()
		rc, ok := c.respc[correlationID]
		delete(c.respc, correlationID)
		c.mu.Unlock()
		if !ok {
			c.logger.Warn(
				"msg", "response to unknown request",
				"correlationID", correlationID)
			continue
		}

		select {
		case <-c.stop:
			stop(false, ErrClosed)
		case rc <- b:
		}
		close(rc)
	}
}

// respWaiter register listener to response message with given correlationID
// and return channel that single response message will be pushed to once it
// will arrive.
// After pushing response message, channel is closed.
//
// Upon connection close, all unconsumed channels are closed.
func (c *connection) respWaiter(correlationID int32) (respc chan []byte, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.stopErr != nil {
		return nil, c.stopErr
	}
	if _, ok := c.respc[correlationID]; ok {
		c.logger.Error("msg", "correlation conflict", "correlationID", correlationID)
		return nil, fmt.Errorf("correlation conflict: %d", correlationID)
	}
	respc = make(chan []byte)
	c.respc[correlationID] = respc
	return respc, nil
}

// releaseWaiter removes response channel from waiters pool and close it.
// Calling this method for unknown correlationID has no effect.
func (c *connection) releaseWaiter(correlationID int32) {
	c.mu.Lock()
	rc, ok := c.respc[correlationID]
	if ok {
		delete(c.respc, correlationID)
		close(rc)
	}
	c.mu.Unlock()
}

// Close close underlying transport connection and cancel all pending response
// waiters.
func (c *connection) Close() error {
	c.mu.Lock()
	if c.stopErr == nil {
		c.stopErr = ErrClosed
		close(c.stop)
	}
	c.mu.Unlock()
	return c.rw.Close()
}

type request interface {
	WriteTo(io.Writer) (int64, error)
}

func (c *connection) writeReq(req request, timeout time.Duration) error {
	errors := make(chan error)
	go func() {
		defer close(errors)
		if err := c.rw.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			c.logger.Error("msg", "SetWriteDeadline failed",
				"error", err)
			errors <- err
			return
		}
		if _, err := req.WriteTo(c.rw); err != nil {
			c.logger.Error("msg", "cannot write", "error", err)
			errors <- err
			return
		}
	}()
	select {
	case err := <-errors:
		return err
	case <-time.After(timeout):
		return proto.ErrRequestTimeout
	}
}

// Metadata sends given metadata request to kafka node and returns related
// metadata response.
// Calling this method on closed connection will always return ErrClosed.
func (c *connection) Metadata(ctx context.Context, timeout time.Duration, req *proto.MetadataReq) (*proto.MetadataResp, error) {
	req.CorrelationID = c.nextID()

	respc, err := c.respWaiter(req.CorrelationID)
	if err != nil {
		c.logger.Error("msg", "failed waiting for response", "error", err)
		return nil, fmt.Errorf("wait for response: %s", err)
	}

	if err := c.writeReq(req, timeout); err != nil {
		c.logger.Error("msg", "writeReq failed",
			"error", err)
		c.releaseWaiter(req.CorrelationID)
		return nil, err
	}
	select {
	case b, ok := <-respc:
		if !ok {
			return nil, c.stopErr
		}
		return proto.ReadMetadataResp(bytes.NewReader(b), req.Version)
	case <-time.After(timeout):
		c.releaseWaiter(req.CorrelationID)
		return nil, proto.ErrRequestTimeout
	case <-ctx.Done():
		c.releaseWaiter(req.CorrelationID)
		return nil, ctx.Err()
	}
}

// Produce sends given produce request to kafka node and returns related
// response. Sending request with no ACKs flag will result with returning nil
// right after sending request, without waiting for response.
// Calling this method on closed connection will always return ErrClosed.
func (c *connection) Produce(ctx context.Context, timeout time.Duration, req *proto.ProduceReq) (*proto.ProduceResp, error) {
	req.CorrelationID = c.nextID()

	if req.RequiredAcks == proto.RequiredAcksNone {
		if err := c.rw.SetWriteDeadline(time.Now().Add(timeout)); err != nil {
			c.logger.Error("msg", "SetWriteDeadline failed",
				"error", err)
			c.releaseWaiter(req.CorrelationID)
			return nil, err
		}
		_, err := req.WriteTo(c.rw)
		return nil, err
	}

	respc, err := c.respWaiter(req.CorrelationID)
	if err != nil {
		c.logger.Error("msg", "failed waiting for response", "error", err)
		return nil, fmt.Errorf("wait for response: %s", err)
	}

	if err := c.writeReq(req, timeout); err != nil {
		c.logger.Error("msg", "writeReq failed",
			"error", err)
		c.releaseWaiter(req.CorrelationID)
		return nil, err
	}
	select {
	case b, ok := <-respc:
		if !ok {
			return nil, c.stopErr
		}
		return proto.ReadProduceResp(bytes.NewReader(b))
	case <-time.After(timeout):
		c.releaseWaiter(req.CorrelationID)
		return nil, proto.ErrRequestTimeout
	case <-ctx.Done():
		c.releaseWaiter(req.CorrelationID)
		return nil, ctx.Err()
	}
}

// Fetch sends given fetch request to kafka node and returns related response.
// Calling this method on closed connection will always return ErrClosed.
func (c *connection) Fetch(ctx context.Context, timeout time.Duration, req *proto.FetchReq) (*proto.FetchResp, error) {
	req.CorrelationID = c.nextID()

	respc, err := c.respWaiter(req.CorrelationID)
	if err != nil {
		c.logger.Error("msg", "failed waiting for response", "error", err)
		return nil, fmt.Errorf("wait for response: %s", err)
	}

	if err := c.writeReq(req, timeout); err != nil {
		c.logger.Error("msg", "writeReq failed",
			"error", err)
		c.releaseWaiter(req.CorrelationID)
		return nil, err
	}
	select {
	case b, ok := <-respc:
		if !ok {
			return nil, c.stopErr
		}
		resp, err := proto.ReadFetchResp(bytes.NewReader(b))
		if err != nil {
			return nil, err
		}

		// Compressed messages are returned in full batches for efficiency
		// (the broker doesn't need to decompress).
		// This means that it's possible to get some leading messages
		// with a smaller offset than requested. Trim those.
		for ti := range resp.Topics {
			topic := &resp.Topics[ti]
			reqTopic := &req.Topics[ti]
			for pi := range topic.Partitions {
				partition := &topic.Partitions[pi]
				reqPartition := &reqTopic.Partitions[pi]
				i := 0
				for _, msg := range partition.Messages {
					if msg.Offset >= reqPartition.FetchOffset {
						break
					}
					i++
				}
				partition.Messages = partition.Messages[i:]
			}
		}
		return resp, nil
	case <-time.After(timeout):
		c.releaseWaiter(req.CorrelationID)
		return nil, proto.ErrRequestTimeout
	case <-ctx.Done():
		c.releaseWaiter(req.CorrelationID)
		return nil, ctx.Err()
	}
}

// Offset sends given offset request to kafka node and returns related response.
// Calling this method on closed connection will always return ErrClosed.
func (c *connection) Offset(ctx context.Context, timeout time.Duration, req *proto.OffsetReq) (*proto.OffsetResp, error) {
	req.CorrelationID = c.nextID()

	respc, err := c.respWaiter(req.CorrelationID)
	if err != nil {
		c.logger.Error("msg", "failed waiting for response", "error", err)
		return nil, fmt.Errorf("wait for response: %s", err)
	}

	// TODO(husio) documentation is not mentioning this directly, but I assume
	// -1 is for non node clients
	req.ReplicaID = -1
	if err := c.writeReq(req, timeout); err != nil {
		c.logger.Error("msg", "writeReq failed",
			"error", err)
		c.releaseWaiter(req.CorrelationID)
		return nil, err
	}
	select {
	case b, ok := <-respc:
		if !ok {
			return nil, c.stopErr
		}
		return proto.ReadOffsetResp(bytes.NewReader(b))
	case <-time.After(timeout):
		c.releaseWaiter(req.CorrelationID)
		return nil, proto.ErrRequestTimeout
	case <-ctx.Done():
		c.releaseWaiter(req.CorrelationID)
		return nil, ctx.Err()
	}
}

func (c *connection) ConsumerMetadata(ctx context.Context, timeout time.Duration, req *proto.ConsumerMetadataReq) (*proto.ConsumerMetadataResp, error) {
	req.CorrelationID = c.nextID()

	respc, err := c.respWaiter(req.CorrelationID)
	if err != nil {
		c.logger.Error("msg", "failed waiting for response", "error", err)
		return nil, fmt.Errorf("wait for response: %s", err)
	}
	if err := c.writeReq(req, timeout); err != nil {
		c.logger.Error("msg", "writeReq failed",
			"error", err)
		c.releaseWaiter(req.CorrelationID)
		return nil, err
	}
	select {
	case b, ok := <-respc:
		if !ok {
			return nil, c.stopErr
		}
		return proto.ReadConsumerMetadataResp(bytes.NewReader(b))
	case <-time.After(timeout):
		c.releaseWaiter(req.CorrelationID)
		return nil, proto.ErrRequestTimeout
	case <-ctx.Done():
		c.releaseWaiter(req.CorrelationID)
		return nil, ctx.Err()
	}
}

func (c *connection) OffsetCommit(ctx context.Context, timeout time.Duration, req *proto.OffsetCommitReq) (*proto.OffsetCommitResp, error) {
	req.CorrelationID = c.nextID()

	respc, err := c.respWaiter(req.CorrelationID)
	if err != nil {
		c.logger.Error("msg", "failed waiting for response", "error", err)
		return nil, fmt.Errorf("wait for response: %s", err)
	}
	if err := c.writeReq(req, timeout); err != nil {
		c.logger.Error("msg", "writeReq failed",
			"error", err)
		c.releaseWaiter(req.CorrelationID)
		return nil, err
	}
	select {
	case b, ok := <-respc:
		if !ok {
			return nil, c.stopErr
		}
		return proto.ReadOffsetCommitResp(bytes.NewReader(b))
	case <-time.After(timeout):
		c.releaseWaiter(req.CorrelationID)
		return nil, proto.ErrRequestTimeout
	case <-ctx.Done():
		c.releaseWaiter(req.CorrelationID)
		return nil, ctx.Err()
	}
}

func (c *connection) OffsetFetch(ctx context.Context, timeout time.Duration, req *proto.OffsetFetchReq) (*proto.OffsetFetchResp, error) {
	req.CorrelationID = c.nextID()

	respc, err := c.respWaiter(req.CorrelationID)
	if err != nil {
		c.logger.Error("msg", "failed waiting for response", "error", err)
		return nil, fmt.Errorf("wait for response: %s", err)
	}
	if err := c.writeReq(req, timeout); err != nil {
		c.logger.Error("msg", "writeReq failed",
			"error", err)
		c.releaseWaiter(req.CorrelationID)
		return nil, err
	}
	select {
	case b, ok := <-respc:
		if !ok {
			return nil, c.stopErr
		}
		return proto.ReadOffsetFetchResp(bytes.NewReader(b))
	case <-time.After(timeout):
		c.releaseWaiter(req.CorrelationID)
		return nil, proto.ErrRequestTimeout
	case <-ctx.Done():
		c.releaseWaiter(req.CorrelationID)
		return nil, ctx.Err()
	}
}

func (c *connection) DeleteTopics(ctx context.Context, timeout time.Duration, req *proto.DeleteTopicsReq) (*proto.DeleteTopicsResp, error) {
	req.CorrelationID = c.nextID()

	respc, err := c.respWaiter(req.CorrelationID)
	if err != nil {
		c.logger.Error("msg", "failed waiting for response", "error", err)
		return nil, fmt.Errorf("wait for response: %s", err)
	}
	if err := c.writeReq(req, timeout); err != nil {
		c.logger.Error("msg", "writeReq failed",
			"error", err)
		c.releaseWaiter(req.CorrelationID)
		return nil, err
	}
	select {
	case b, ok := <-respc:
		if !ok {
			return nil, c.stopErr
		}
		return proto.ReadDeleteTopicsResp(bytes.NewReader(b))
	case <-time.After(timeout):
		c.releaseWaiter(req.CorrelationID)
		return nil, proto.ErrRequestTimeout
	case <-ctx.Done():
		c.releaseWaiter(req.CorrelationID)
		return nil, ctx.Err()
	}
}

func (c *connection) DescribeConfigs(ctx context.Context, timeout time.Duration, req *proto.DescribeConfigsReq) (*proto.DescribeConfigsResp, error) {
	req.CorrelationID = c.nextID()

	respc, err := c.respWaiter(req.CorrelationID)
	if err != nil {
		c.logger.Error("msg", "failed waiting for response", "error", err)
		return nil, fmt.Errorf("wait for response: %s", err)
	}
	if err := c.writeReq(req, timeout); err != nil {
		c.logger.Error("msg", "writeReq failed",
			"error", err)
		c.releaseWaiter(req.CorrelationID)
		return nil, err
	}
	select {
	case b, ok := <-respc:
		if !ok {
			return nil, c.stopErr
		}
		return proto.ReadDescribeConfigsResp(bytes.NewReader(b))
	case <-time.After(timeout):
		c.releaseWaiter(req.CorrelationID)
		return nil, proto.ErrRequestTimeout
	case <-ctx.Done():
		c.releaseWaiter(req.CorrelationID)
		return nil, ctx.Err()
	}
}
