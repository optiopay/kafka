package kafka

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"sync"
	"syscall"
	"time"

	"github.com/husio/kafka/proto"
)

const (
	// StartOffsetNewest configures consumer to fetch messages produced after
	// creating the consumer.
	StartOffsetNewest = -1

	// StartOffsetOldest configures consumer to fetch starting from the oldest
	// message available
	StartOffsetOldest = -2
)

var (
	// Returned by consumer Fetch when retry limit was set and exceeded during
	// single function call
	ErrNoData = errors.New("no data")

	// make sure interfaces are implemented
	_ Client  = &Broker{}
	_ Fetcher = &Consumer{}
	_ Sender  = &Producer{}
)

// Client is interface implemented by Broker.
type Client interface {
	Producer(ProducerConf) Sender
	Consumer(ConsumerConf) (Fetcher, error)
	OffsetEarliest(string, int32) (int64, error)
	OffsetLatest(string, int32) (int64, error)
	Close()
}

// Fetcher is interface implemented by Consumer.
type Fetcher interface {
	Fetch() (*proto.Message, error)
}

// Sender is insterface implemented by Producer.
type Sender interface {
	Produce(string, int32, ...*proto.Message) (int64, error)
}

// Logger is insterface implemented by log.Logger
type Logger interface {
	Print(...interface{})
	Printf(string, ...interface{})
}

type clusterMetadata struct {
	created   time.Time
	nodes     map[int32]string // node ID to address
	endpoints map[string]int32 // topic:partition to leader node ID
}

type BrokerConf struct {
	// Kafka client ID.
	ClientID string

	// LeaderRetryLimit limits number of connection attempts to single node
	// before failing. Use LeaderRetryWait to control wait time between
	// retries. Defaults to 10.
	LeaderRetryLimit int

	// LeaderRetryWait controls duration of wait between trying to connect to
	// single node after failure. Defaults to 500ms.
	// Timeout on connection is controlled by DialTimeout setting.
	LeaderRetryWait time.Duration

	// Any new connection dial timeout. By default 10 seconds.
	DialTimeout time.Duration

	// Logger used by the broker. By default all messages are dropped.
	Log Logger
}

func NewBrokerConf(clientID string) BrokerConf {
	return BrokerConf{
		ClientID:         clientID,
		DialTimeout:      time.Second * 10,
		LeaderRetryLimit: 10,
		LeaderRetryWait:  time.Millisecond * 500,
		Log:              log.New(ioutil.Discard, "kafka", log.LstdFlags),
	}
}

// Broker is abstract connection to kafka cluster, managing connections to all
// kafka nodes.
type Broker struct {
	conf BrokerConf

	mu       sync.Mutex
	metadata clusterMetadata
	conns    map[int32]*connection
}

// Dial connects to any node from given list of kafka addresses and after
// successful metadata fetch, returns broker.
// Returned broker is not initially connected to any kafka node.
func Dial(nodeAddresses []string, conf BrokerConf) (*Broker, error) {
	broker := &Broker{
		conf:  conf,
		conns: make(map[int32]*connection),
	}

	for _, addr := range nodeAddresses {
		conn, err := NewConnection(addr, conf.DialTimeout)
		if err != nil {
			conf.Log.Printf("could not connect to %s: %s", addr, err)
			continue
		}
		defer func(c *connection) {
			_ = c.Close()
		}(conn)
		resp, err := conn.Metadata(&proto.MetadataReq{
			ClientID: broker.conf.ClientID,
			Topics:   nil,
		})
		if err != nil {
			conf.Log.Printf("could not fetch metadata from %s: %s", addr, err)
			continue
		}
		broker.rewriteMetadata(resp)
		return broker, nil
	}
	return nil, errors.New("could not connect")
}

// Close is closing broker and all active kafka nodes connections.
func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for nodeID, conn := range b.conns {
		if err := conn.Close(); err != nil {
			b.conf.Log.Printf("failed closing node %d connection: %s", nodeID, err)
		}
	}
}

func (b *Broker) Metadata() (*proto.MetadataResp, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.fetchMetadata()
}

// refreshMetadata is requesting metadata information from any node and refresh
// internal cached representation.
// Because it's changing internal state, this method requires lock protection,
// but it does not acquire nor release lock itself.
func (b *Broker) refreshMetadata() error {
	meta, err := b.fetchMetadata()
	if err == nil {
		b.rewriteMetadata(meta)
	}
	return err
}

// fetchMetadata is requesting metadata information from any node and return
// protocol response if successful
// Because it's using metadata information to find node connections it's not
// thread safe and using it require locking.
func (b *Broker) fetchMetadata() (*proto.MetadataResp, error) {
	checkednodes := make(map[int32]bool)

	// try all existing connections first
	for nodeID, conn := range b.conns {
		checkednodes[nodeID] = true
		resp, err := conn.Metadata(&proto.MetadataReq{
			ClientID: b.conf.ClientID,
			Topics:   nil,
		})
		if err != nil {
			b.conf.Log.Printf("cannot fetch metadata from node %d: %s", nodeID, err)
			continue
		}
		return resp, nil
	}

	// try all nodes that we know of that we're not connected to
	for nodeID, addr := range b.metadata.nodes {
		if _, ok := checkednodes[nodeID]; ok {
			continue
		}
		conn, err := NewConnection(addr, b.conf.DialTimeout)
		if err != nil {
			b.conf.Log.Printf("could not connect to %s: %s", addr, err)
			continue
		}
		// we had no active connection to this node, so most likely we don't need it
		defer func(c *connection) {
			_ = c.Close()
		}(conn)

		resp, err := conn.Metadata(&proto.MetadataReq{
			ClientID: b.conf.ClientID,
			Topics:   nil,
		})
		if err != nil {
			b.conf.Log.Printf("cannot fetch metadata from node %d: %s", nodeID, err)
			continue
		}
		return resp, nil
	}

	return nil, errors.New("cannot fetch metadata")
}

// rewriteMetadata creates new internal metadata representation using data from
// given response. It's call has to be protected with lock.
func (b *Broker) rewriteMetadata(resp *proto.MetadataResp) {
	b.conf.Log.Printf("rewriting metadata created %s ago", time.Now().Sub(b.metadata.created))
	b.metadata = clusterMetadata{
		created:   time.Now(),
		nodes:     make(map[int32]string),
		endpoints: make(map[string]int32),
	}
	for _, node := range resp.Brokers {
		b.metadata.nodes[node.NodeID] = fmt.Sprintf("%s:%d", node.Host, node.Port)
	}
	for _, topic := range resp.Topics {
		for _, part := range topic.Partitions {
			b.metadata.endpoints[fmt.Sprintf("%s:%d", topic.Name, part.ID)] = part.Leader
		}
	}
}

// leaderConnection returns connection to leader for given partition. If
// connection does not exist, broker will try to connect first and add store
// connection for any further use.
// Failed connection retry is controlled by broker configuration.
func (b *Broker) leaderConnection(topic string, partition int32) (conn *connection, err error) {
	endpoint := fmt.Sprintf("%s:%d", topic, partition)

	for retry := 0; retry < b.conf.LeaderRetryLimit; retry++ {
		b.mu.Lock()

		nodeID, ok := b.metadata.endpoints[endpoint]
		if !ok {
			if err := b.refreshMetadata(); err != nil {
				b.conf.Log.Printf("cannot refresh metadata: %s", err)
				b.mu.Unlock()
				return nil, err
			}
			nodeID, ok = b.metadata.endpoints[endpoint]
			if !ok {
				b.mu.Unlock()
				return nil, proto.ErrUnknownTopicOrPartition
			}
		}

		conn, ok = b.conns[nodeID]
		if !ok {
			addr, ok := b.metadata.nodes[nodeID]
			if !ok {
				b.conf.Log.Printf("no information about node %d", nodeID)
				b.mu.Unlock()
				return nil, proto.ErrBrokerNotAvailable
			}
			conn, err = NewConnection(addr, b.conf.DialTimeout)
			if err != nil {
				b.conf.Log.Printf("cannot connect to node %s: %s", addr, err)
				b.mu.Unlock()
				time.Sleep(b.conf.LeaderRetryWait)
				continue
			}
			b.conns[nodeID] = conn
		}
		b.mu.Unlock()
		return conn, nil
	}
	return nil, err
}

// closeDeadConnection is closing and removing any reference to given
// connection. Because we remove dead connection, additional request to refresh
// metadata is made
func (b *Broker) closeDeadConnection(conn *connection) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for nid, c := range b.conns {
		if c == conn {
			delete(b.conns, nid)
			_ = c.Close()
			_ = b.refreshMetadata()
			return
		}
	}
}

// offset will return offset value for given partition. Use timems to specify
// which offset value should be returned.
func (b *Broker) offset(topic string, partition int32, timems int64) (offset int64, err error) {
	conn, err := b.leaderConnection(topic, partition)
	if err != nil {
		return 0, err
	}
	resp, err := conn.Offset(&proto.OffsetReq{
		ClientID:  b.conf.ClientID,
		ReplicaID: -1, // any client
		Topics: []proto.OffsetReqTopic{
			proto.OffsetReqTopic{
				Name: topic,
				Partitions: []proto.OffsetReqPartition{
					proto.OffsetReqPartition{
						ID:         partition,
						TimeMs:     timems,
						MaxOffsets: 2,
					},
				},
			},
		},
	})
	if err != nil {
		return 0, err
	}
	found := false
	for _, t := range resp.Topics {
		if t.Name != topic {
			b.conf.Log.Printf("unexpected topic information received: %s (expecting %s)", t.Name, topic)
			continue
		}
		for _, part := range t.Partitions {
			if part.ID != partition {
				b.conf.Log.Printf("unexpected partition information received: %s:%d (expecting %d)", t.Name, part.ID, partition)
				continue
			}
			found = true
			// happends when there are no messages
			if len(part.Offsets) == 0 {
				offset = 0
			} else {
				offset = part.Offsets[0]
			}
			err = part.Err
		}
	}
	if !found {
		return 0, errors.New("incomplete fetch response")
	}
	return offset, err
}

// OffsetEarliest returns offset of the oldest message available in given partition.
func (b *Broker) OffsetEarliest(topic string, partition int32) (offset int64, err error) {
	return b.offset(topic, partition, -2)
}

// OffsetLatest return offset of the next message produced in given partition
func (b *Broker) OffsetLatest(topic string, partition int32) (offset int64, err error) {
	return b.offset(topic, partition, -1)
}

type ProducerConf struct {
	// Timeout of single produce request. By default, 5 seconds.
	RequestTimeout time.Duration

	// Message ACK configuration. Use proto.RequiredAcksAll to require all
	// servers to write, proto.RequiredAcksLocal to wait only for leader node
	// answer or proto.RequiredAcksNone to not wait for any response.
	// Setting this to any other, greater than zero value will make producer to
	// wait for given number of servers to confirm write before returning.
	RequiredAcks int16

	// RetryLimit specify how many times message producing should be retried in
	// case of failure, before returning the error to the caller. By default
	// set to 5.
	RetryLimit int

	// RetryWait specify wait duration before produce retry after failure. By
	// default set to 200ms.
	RetryWait time.Duration

	// Logger used by producer. By default, reuse logger assigned to broker.
	Log Logger
}

// NewProducerConf return default producer configuration
func NewProducerConf() ProducerConf {
	return ProducerConf{
		RequestTimeout: time.Second * 5,
		RequiredAcks:   proto.RequiredAcksAll,
		RetryLimit:     5,
		RetryWait:      time.Millisecond * 200,
		Log:            nil,
	}
}

// Producer is link to broker with extra configuration.
type Producer struct {
	conf   ProducerConf
	broker *Broker
}

// Producer returns new producer instance, bound to broker.
func (b *Broker) Producer(conf ProducerConf) Sender {
	if conf.Log == nil {
		conf.Log = b.conf.Log
	}
	return &Producer{
		conf:   conf,
		broker: b,
	}
}

// Produce writes messages to given destination. Write within single Produce
// call are atomic, meaning either all or none of them are written to kafka.
// Produce can retry sending request on common errors. This behaviour can be
// configured with RetryLimit and RetryWait producer configuration attributes.
//
// Upon successful produce call, messages Offset field is updated.
func (p *Producer) Produce(topic string, partition int32, messages ...*proto.Message) (offset int64, err error) {

retryLoop:
	for retry := 0; retry < p.conf.RetryLimit; retry++ {
		offset, err = p.produce(topic, partition, messages...)
		switch err {
		case proto.ErrLeaderNotAvailable, proto.ErrNotLeaderForPartition, proto.ErrBrokerNotAvailable, proto.ErrUnknownTopicOrPartition:
			p.conf.Log.Printf("failed to produce messages (%d): %s", retry, err)
			time.Sleep(p.conf.RetryWait)
			// TODO(husio) possible thundering herd
			if err := p.broker.refreshMetadata(); err != nil {
				p.conf.Log.Printf("failed to refresh metadata: %s", err)
			}
		case io.EOF, syscall.EPIPE:
			// p.produce call is closing connection when this error shows up,
			// but it's also returning it so that retry loop can count this
			// case
			continue
		case proto.ErrRequestTimeout:
			p.conf.Log.Printf("failed to produce messages (%d): %s", retry, err)
			time.Sleep(p.conf.RetryWait)
		default:
			break retryLoop
		}
	}

	if err == nil {
		// offset is the offset value of first published messages
		for i, msg := range messages {
			msg.Offset = int64(i) + offset
		}
	}

	return offset, err
}

// produce send produce request to leader for given destination.
func (p *Producer) produce(topic string, partition int32, messages ...*proto.Message) (offset int64, err error) {
	conn, err := p.broker.leaderConnection(topic, partition)
	if err != nil {
		return 0, err
	}

	req := proto.ProduceReq{
		ClientID:     p.broker.conf.ClientID,
		RequiredAcks: p.conf.RequiredAcks,
		Timeout:      p.conf.RequestTimeout,
		Topics: []proto.ProduceReqTopic{
			proto.ProduceReqTopic{
				Name: topic,
				Partitions: []proto.ProduceReqPartition{
					proto.ProduceReqPartition{
						ID:       partition,
						Messages: messages,
					},
				},
			},
		},
	}

	resp, err := conn.Produce(&req)
	if err != nil {
		if err == io.EOF || err == syscall.EPIPE {
			// Connection is broken, so should be closed, but the error is
			// still valid and should be returned so that retry mechanism have
			// chance to react.
			p.broker.closeDeadConnection(conn)
		}
		return 0, err
	}

	// we expect single partition response
	found := false
	for _, t := range resp.Topics {
		if t.Name != topic {
			p.conf.Log.Printf("unexpected topic information received: %s", t.Name)
			continue
		}
		for _, part := range t.Partitions {
			if part.ID != partition {
				p.conf.Log.Printf("unexpected partition information received: %s:%d", t.Name, part.ID)
				continue
			}
			found = true
			offset = part.Offset
			err = part.Err
		}
	}

	if !found {
		return 0, errors.New("incomplete produce response")
	}
	return offset, err
}

type ConsumerConf struct {
	// Topic name that should be consumed
	Topic string

	// Partition ID that should be consumed.
	Partition int32

	// RequestTimeout controlls fetch request timeout.This operation is
	// blocking the whole connection, so it should always be set to small
	// value. By default it's set to 50ms.
	// To control fetch function timeout use RetryLimit and RetryWait.
	RequestTimeout time.Duration

	// RetryLimit limits fetching messages given amount of times before
	// returning ErrNoData error. By default set to -1, which turns this limit
	// off.
	RetryLimit int

	// RetryWait controls duration of wait between fetch request calls, when
	// no data was returned. Defaults to 50ms.
	RetryWait time.Duration

	// RetryErrLimit limits messages fetch retry upon failure. By default 10.
	RetryErrLimit int

	// RetryErrWait controls wait duration between retries after failed fetch
	// request. By default 500ms.
	RetryErrWait time.Duration

	// MinFetchSize is minimum size of messages to fetch in bytes. By default
	// set to 1 to fetch any message available.
	MinFetchSize int32

	// MaxFetchSize is maximum size of data that can be send by kafka node to
	// consumer. By default set to 2000000 bytes.
	MaxFetchSize int32

	// Consumer cursor starting point. Set to StartOffsetNewest to receive only
	// newly created messages or StartOffsetOldest to read everything. Assign
	// any offset value to manually set cursor -- consuming starts with message
	// which offset is equal to given value. Defaults to StartOffsetOldest
	StartOffset int64

	// Logger used by consumer. By default, reuse logger assigned to broker.
	Log Logger
}

// NewConsumerConf return default consumer configuration.
func NewConsumerConf(topic string, partition int32) ConsumerConf {
	return ConsumerConf{
		Topic:          topic,
		Partition:      partition,
		RequestTimeout: time.Millisecond * 50,
		RetryLimit:     -1,
		RetryWait:      time.Millisecond * 50,
		RetryErrLimit:  10,
		RetryErrWait:   time.Millisecond * 500,
		MinFetchSize:   1,
		MaxFetchSize:   2000000,
		StartOffset:    StartOffsetOldest,
		Log:            nil,
	}
}

// Consumer is representing single partition reading buffer. Consumer is also
// providing limited failures handling and message filtering.
type Consumer struct {
	broker *Broker
	conn   *connection
	conf   ConsumerConf
	offset int64

	mu     sync.Mutex
	msgbuf []*proto.Message
}

// Consumer creates new consumer instance, bound to broker.
func (b *Broker) Consumer(conf ConsumerConf) (consumer Fetcher, err error) {
	conn, err := b.leaderConnection(conf.Topic, conf.Partition)
	if err != nil {
		return nil, err
	}
	if conf.Log == nil {
		conf.Log = b.conf.Log
	}
	offset := conf.StartOffset
	if conf.StartOffset < 0 {
		switch conf.StartOffset {
		case StartOffsetNewest:
			off, err := b.OffsetLatest(conf.Topic, conf.Partition)
			if err != nil {
				return nil, err
			}
			// latest offset that next produced message will get, but consumer
			// offset should be set to the last returned message offset
			offset = off - 1
		case StartOffsetOldest:
			off, err := b.OffsetEarliest(conf.Topic, conf.Partition)
			if err != nil {
				return nil, err
			}
			offset = off
			// this is stange kafka thing: asking about message with offset one
			// higher than the last existing message is fine, except when there
			// are no messages in the log -- in such case you should always ask
			// about offset 0.
			// Set offset -1 to make fetch request 0 offset message when there
			// are none.
			if off == 0 {
				offset = -1
			}
		default:
			return nil, fmt.Errorf("invalid start offset: %d", conf.StartOffset)
		}
	}
	consumer = &Consumer{
		broker: b,
		conn:   conn,
		conf:   conf,
		msgbuf: make([]*proto.Message, 0),
		offset: offset,
	}
	return consumer, nil
}

// Fetch is returning single message from consumed partition. Consumer can
// retry fetching messages even if responses return no new data. Retry
// behaviour can be configured through RetryLimit and RetryWait consumer
// parameters.
//
// Fetch can retry sending request on common errors. This behaviour can be
// configured with RetryErrLimit and RetryErrWait consumer configuration
// attributes.
func (c *Consumer) Fetch() (*proto.Message, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	var retry int
	for len(c.msgbuf) == 0 {
		messages, err := c.fetch()
		if err != nil {
			return nil, err
		}

		// check first messages if any of them has index lower than requested,
		// this can happen because of kafka optimizations
		toSkip := 0
		for toSkip < len(messages) {
			if messages[toSkip].Offset >= c.offset {
				break
			}
			toSkip++
		}

		// messages crc is checked by underlying connection so no need to check
		// those again

		if len(messages) == toSkip {
			c.conf.Log.Printf("none of %d fetched messages is valid", toSkip)
		}
		// ignore all messages that are of index lower than requested
		c.msgbuf = messages[toSkip:]

		if len(c.msgbuf) == 0 {
			if c.conf.RetryWait > 0 {
				time.Sleep(c.conf.RetryWait)
			}
			retry += 1
			if c.conf.RetryLimit != -1 && retry > c.conf.RetryLimit {
				return nil, ErrNoData
			}
		}
	}

	msg := c.msgbuf[0]
	c.msgbuf = c.msgbuf[1:]
	c.offset = msg.Offset
	return msg, nil
}

// Fetch and return next batch of messages. In case of certain set of errors,
// retry sending fetch request. Retry behaviour can be configured with
// RetryErrLimit and RetryErrWait consumer configuration attributes.
func (c *Consumer) fetch() ([]*proto.Message, error) {
	req := proto.FetchReq{
		ClientID:    c.broker.conf.ClientID,
		MaxWaitTime: c.conf.RequestTimeout,
		MinBytes:    c.conf.MinFetchSize,
		Topics: []proto.FetchReqTopic{
			proto.FetchReqTopic{
				Name: c.conf.Topic,
				Partitions: []proto.FetchReqPartition{
					proto.FetchReqPartition{
						ID:          c.conf.Partition,
						FetchOffset: c.offset + 1,
						MaxBytes:    c.conf.MaxFetchSize,
					},
				},
			},
		},
	}

	for retry := 0; ; retry++ {
		if c.conn == nil {
			conn, err := c.broker.leaderConnection(c.conf.Topic, c.conf.Partition)
			if err != nil {
				if retry == c.conf.RetryErrLimit {
					return nil, err
				}
				continue
			}
			c.conn = conn
		}

		resp, err := c.conn.Fetch(&req)
		if err != nil && retry == c.conf.RetryErrLimit {
			return nil, err
		}

		switch err {
		case proto.ErrLeaderNotAvailable, proto.ErrNotLeaderForPartition, proto.ErrBrokerNotAvailable:
			c.conf.Log.Printf("failed to fetch messages (%d): %s", retry, err)
			time.Sleep(c.conf.RetryErrWait)
			// TODO(husio) possible thundering herd
			if err := c.broker.refreshMetadata(); err != nil {
				c.conf.Log.Printf("failed to refresh metadata: %s", err)
			}
			continue
		case io.EOF, syscall.EPIPE:
			c.broker.closeDeadConnection(c.conn)
			c.conn = nil
			continue
		case nil:
			// everything's fine, proceed
		default:
			return nil, err
		}

		for _, topic := range resp.Topics {
			if topic.Name != c.conf.Topic {
				c.conf.Log.Printf("unexpected topic information received: %s (expecting %s)", topic.Name, c.conf.Topic)
				continue
			}
			for _, part := range topic.Partitions {
				if part.ID != c.conf.Partition {
					c.conf.Log.Printf("unexpected partition information received: %s:%d", topic.Name, part.ID)
					continue
				}
				return part.Messages, nil
			}
		}
		return nil, errors.New("incomplete fetch response")
	}
}
