package kafka

import (
	"bufio"
	"bytes"
	"errors"
	"net"
	"time"

	"github.com/optiopay/kafka/proto"
)

const (
	// Size of request-response mapping array for every connection instance.
	//
	// Because client is responsible for setting up message correlation ID, we
	// can use lock free array instead of lock protected map. The only
	// requirement is that number of unanswered requests at any time has to be
	// smalled than the response buffer size.
	responseBufferSize int32 = 256
)

// ErrClosed is returned as result of any request made using closed connection.
var ErrClosed = errors.New("closed")

// Low level abstraction over TCP connection to one of kafka nodes.
type connection struct {
	conn    net.Conn
	stopErr error
	stop    chan struct{}
	nextID  chan int32
	respc   [responseBufferSize]chan []byte
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
			if id == responseBufferSize {
				id = 1
			}
		}
	}
}

// readRespLoop constantly reading response messages from the socket and after
// partial parsing, sends byte representation of the whole message to request
// sending process.
func (c *connection) readRespLoop() {
	for i := range c.respc {
		c.respc[i] = make(chan []byte, 1)
	}
	defer func() {
		for _, cc := range c.respc {
			close(cc)
		}
	}()

	rd := bufio.NewReader(c.conn)
	for {
		correlationID, b, err := proto.ReadResp(rd)
		if err != nil {
			c.stopErr = err
			return
		}

		select {
		case <-c.stop:
			c.stopErr = ErrClosed
		case c.respc[correlationID] <- b:
		}
	}
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
		return nil, c.stopErr
	}

	if _, err := req.WriteTo(c.conn); err != nil {
		return nil, err
	}
	b, ok := <-c.respc[req.CorrelationID]
	if !ok {
		return nil, c.stopErr
	}
	return proto.ReadMetadataResp(bytes.NewReader(b))
}

// Produce sends given produce request to kafka node and returns related
// response.
// Calling this method on closed connection will always return ErrClosed.
func (c *connection) Produce(req *proto.ProduceReq) (*proto.ProduceResp, error) {
	var ok bool
	if req.CorrelationID, ok = <-c.nextID; !ok {
		return nil, c.stopErr
	}

	if _, err := req.WriteTo(c.conn); err != nil {
		return nil, err
	}
	b, ok := <-c.respc[req.CorrelationID]
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

	if _, err := req.WriteTo(c.conn); err != nil {
		return nil, err
	}
	b, ok := <-c.respc[req.CorrelationID]
	if !ok {
		return nil, c.stopErr
	}
	return proto.ReadFetchResp(bytes.NewReader(b))
}
