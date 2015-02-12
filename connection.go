package kafka

import (
	"bufio"
	"bytes"
	"errors"
	"net"
	"time"
)

const (
	responseBufferSize int32 = 256
)

var ErrClosed = errors.New("closed")

type connection struct {
	conn    net.Conn
	stopErr error
	stop    chan struct{}
	nextID  chan int32
	respc   [responseBufferSize]chan []byte
}

func newConnection(address string) (*connection, error) {
	conn, err := net.DialTimeout("tcp", address, time.Second*10)
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
		correlationID, b, err := ReadResp(rd)
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

func (c *connection) Metadata(req *MetadataReq) (*MetadataResp, error) {
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
	return ReadMetadataResp(bytes.NewReader(b))
}

func (c *connection) Produce(req *ProduceReq) (*ProduceResp, error) {
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
	return ReadProduceResp(bytes.NewReader(b))
}

func (c *connection) Fetch(req *FetchReq) (*FetchResp, error) {
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
	return ReadFetchResp(bytes.NewReader(b))
}
