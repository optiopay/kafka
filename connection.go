package kafka

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"log"
	"math"
	"net"
	"sync"
	"time"

	"github.com/optiopay/kafka/proto"
)

// ErrClosed is returned as result of any request made using closed connection.
var ErrClosed = errors.New("closed")

// Low level abstraction over TCP connection to one of kafka nodes.
type connection struct {
	conn   net.Conn
	stop   chan struct{}
	nextID chan int32

	mu      sync.Mutex
	respc   map[int32]chan []byte
	stopErr error
}

// newConnection returns new, initialized connection or error
func newConnection(address string, timeout time.Duration) (*connection, error) {
	conn, err := net.DialTimeout("tcp", address, timeout)
	if err != nil {
		return nil, err
	}
	c := &connection{
		stop:   make(chan struct{}),
		nextID: make(chan int32, 4),
		conn:   conn,
		respc:  make(map[int32]chan []byte),
	}
	go c.nextIDLoop()
	go c.readRespLoop()
	return c, nil
}

// nextIDLoop generates correlation IDs, making sure they are always in order
// and within the scope of request-response mapping array.
func (c *connection) nextIDLoop() {
	var id int32 = 1
	for {
		select {
		case <-c.stop:
			close(c.nextID)
			return
		case c.nextID <- id:
			id++
			if id == math.MaxInt32 {
				id = 1
			}
		}
	}
}

// readRespLoop constantly reading response messages from the socket and after
// partial parsing, sends byte representation of the whole message to request
// sending process.
func (c *connection) readRespLoop() {
	defer func() {
		c.mu.Lock()
		for _, cc := range c.respc {
			close(cc)
		}
		c.mu.Unlock()
	}()

	rd := bufio.NewReader(c.conn)
	for {
		correlationID, b, err := proto.ReadResp(rd)
		if err != nil {
			c.mu.Lock()
			c.stopErr = err
			c.mu.Unlock()
			return
		}

		c.mu.Lock()
		rc, ok := c.respc[correlationID]
		delete(c.respc, correlationID)
		c.mu.Unlock()
		if !ok {
			log.Printf("%#v", c.respc)
			log.Panicf("response to unknown request: %d", correlationID)
		}

		select {
		case <-c.stop:
			c.mu.Lock()
			c.stopErr = ErrClosed
			c.mu.Unlock()
		case rc <- b:
		}
	}
}

func (c *connection) respWaiter(correlationID int32) (respc chan []byte, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if _, ok := c.respc[correlationID]; ok {
		return nil, fmt.Errorf("correlation conflict: %d", correlationID)
	}
	respc = make(chan []byte)
	c.respc[correlationID] = respc
	return respc, nil
}

func (c *connection) Close() error {
	close(c.stop)
	return c.conn.Close()
}

// Metadata sends given metadata request to kafka node and returns related
// metadata response.
// Calling this method on closed connection will always return ErrClosed.
func (c *connection) Metadata(req *proto.MetadataReq) (*proto.MetadataResp, error) {
	var ok bool
	if req.CorrelationID, ok = <-c.nextID; !ok {
		c.mu.Lock()
		err := c.stopErr
		c.mu.Unlock()
		return nil, err
	}

	respc, err := c.respWaiter(req.CorrelationID)
	if err != nil {
		return nil, fmt.Errorf("wait for response: %s", err)
	}

	if _, err := req.WriteTo(c.conn); err != nil {
		return nil, err
	}
	b, ok := <-respc
	if !ok {
		c.mu.Lock()
		err := c.stopErr
		c.mu.Unlock()
		return nil, err
	}
	return proto.ReadMetadataResp(bytes.NewReader(b))
}

// Produce sends given produce request to kafka node and returns related
// response. Sending request with no ACKs flag will result with returning nil
// right after sending request, without waiting for response.
// Calling this method on closed connection will always return ErrClosed.
func (c *connection) Produce(req *proto.ProduceReq) (*proto.ProduceResp, error) {
	var ok bool
	if req.CorrelationID, ok = <-c.nextID; !ok {
		return nil, c.stopErr
	}

	if req.RequiredAcks == proto.RequiredAcksNone {
		_, err := req.WriteTo(c.conn)
		return nil, err
	}

	respc, err := c.respWaiter(req.CorrelationID)
	if err != nil {
		return nil, fmt.Errorf("wait for response: %s", err)
	}

	if _, err := req.WriteTo(c.conn); err != nil {
		return nil, err
	}
	b, ok := <-respc
	if !ok {
		return nil, c.stopErr
	}
	return proto.ReadProduceResp(bytes.NewReader(b))
}

// Fetch sends given fetch request to kafka node and returns related response.
// Calling this method on closed connection will always return ErrClosed.
func (c *connection) Fetch(req *proto.FetchReq) (*proto.FetchResp, error) {
	var ok bool
	if req.CorrelationID, ok = <-c.nextID; !ok {
		return nil, c.stopErr
	}

	respc, err := c.respWaiter(req.CorrelationID)
	if err != nil {
		return nil, fmt.Errorf("wait for response: %s", err)
	}

	if _, err := req.WriteTo(c.conn); err != nil {
		return nil, err
	}
	b, ok := <-respc
	if !ok {
		return nil, c.stopErr
	}
	return proto.ReadFetchResp(bytes.NewReader(b))
}

// Offset sends given offset request to kafka node and returns related response.
// Calling this method on closed connection will always return ErrClosed.
func (c *connection) Offset(req *proto.OffsetReq) (*proto.OffsetResp, error) {
	var ok bool
	if req.CorrelationID, ok = <-c.nextID; !ok {
		return nil, c.stopErr
	}

	respc, err := c.respWaiter(req.CorrelationID)
	if err != nil {
		return nil, fmt.Errorf("wait for response: %s", err)
	}

	// TODO(husio) documentation is not mentioning this directly, but I assume
	// -1 is for non node clients
	req.ReplicaID = -1
	if _, err := req.WriteTo(c.conn); err != nil {
		return nil, err
	}
	b, ok := <-respc
	if !ok {
		return nil, c.stopErr
	}
	return proto.ReadOffsetResp(bytes.NewReader(b))
}

func (c *connection) ConsumerMetadata(req *proto.ConsumerMetadataReq) (*proto.ConsumerMetadataResp, error) {
	var ok bool
	if req.CorrelationID, ok = <-c.nextID; !ok {
		return nil, c.stopErr
	}
	respc, err := c.respWaiter(req.CorrelationID)
	if err != nil {
		return nil, fmt.Errorf("wait for response: %s", err)
	}
	if _, err := req.WriteTo(c.conn); err != nil {
		return nil, err
	}
	b, ok := <-respc
	if !ok {
		return nil, c.stopErr
	}
	return proto.ReadConsumerMetadataResp(bytes.NewReader(b))
}

func (c *connection) OffsetCommit(req *proto.OffsetCommitReq) (*proto.OffsetCommitResp, error) {
	var ok bool
	if req.CorrelationID, ok = <-c.nextID; !ok {
		return nil, c.stopErr
	}
	respc, err := c.respWaiter(req.CorrelationID)
	if err != nil {
		return nil, fmt.Errorf("wait for response: %s", err)
	}
	if _, err := req.WriteTo(c.conn); err != nil {
		return nil, err
	}
	b, ok := <-respc
	if !ok {
		return nil, c.stopErr
	}
	return proto.ReadOffsetCommitResp(bytes.NewReader(b))
}

func (c *connection) OffsetFetch(req *proto.OffsetFetchReq) (*proto.OffsetFetchResp, error) {
	var ok bool
	if req.CorrelationID, ok = <-c.nextID; !ok {
		return nil, c.stopErr
	}
	respc, err := c.respWaiter(req.CorrelationID)
	if err != nil {
		return nil, fmt.Errorf("wait for response: %s", err)
	}
	if _, err := req.WriteTo(c.conn); err != nil {
		return nil, err
	}
	b, ok := <-respc
	if !ok {
		return nil, c.stopErr
	}
	return proto.ReadOffsetFetchResp(bytes.NewReader(b))
}
