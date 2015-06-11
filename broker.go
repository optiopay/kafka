package kafka

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math/rand"
	"sync"
	"syscall"
	"time"

	"github.com/optiopay/kafka/proto"
)

const (
	// StartOffsetNewest configures the consumer to fetch messages produced
	// after creating the consumer.
	StartOffsetNewest = -1

	// StartOffsetOldest configures the consumer to fetch starting from the
	// oldest message available.
	StartOffsetOldest = -2
)

var (
	// Returned by consumers on Fetch when the retry limit is set and exceeded.
	ErrNoData = errors.New("no data")

	// Make sure interfaces are implemented
	_ Client            = &Broker{}
	_ Consumer          = &consumer{}
	_ Producer          = &producer{}
	_ OffsetCoordinator = &offsetCoordinator{}
)

// Client is the interface implemented by Broker.
type Client interface {
	Producer(ProducerConf) Producer
	Consumer(ConsumerConf) (Consumer, error)
	OffsetCoordinator(OffsetCoordinatorConf) (OffsetCoordinator, error)
	OffsetEarliest(string, int32) (int64, error)
	OffsetLatest(string, int32) (int64, error)
	Close()
}

// Consumer is the interface that wraps the Consume method.
//
// Consume reads a message from a consumer, returning an error when
// encountered.
type Consumer interface {
	Consume() (*proto.Message, error)
}

// Producer is the interface that wraps the Produce method.
//
// Produce writes the messages to the given topic and partition, returning the
// post-commit offset and any error encountered.  The offset of each message is
// also updated accordingly.
type Producer interface {
	Produce(string, interface{}, ...*proto.Message) (int64, error)
}

// Partitioner is a type of method that selects a partition. Is passed the topic
// and the messages that are about to be produced.
type Partitioner func(string, ...*proto.Message) int

// OffsetCoordinator is the interface which wraps the Commit and Offset methods.
type OffsetCoordinator interface {
	Commit(topic string, partition int32, offset int64) error
	Offset(topic string, partition int32) (offset int64, metadata string, err error)
}

// Logger is the interface used to wrap logging functionality.
type Logger interface {
	Print(...interface{})
	Printf(string, ...interface{})
}

type clusterMetadata struct {
	created        time.Time
	nodes          map[int32]string // node ID to address
	endpoints      map[string]int32 // topic:partition to leader node ID
	partitionCount map[string]int32 // topic to partition count
}

type BrokerConf struct {
	// Kafka client ID.
	ClientID string

	// LeaderRetryLimit limits the number of connection attempts to a single
	// node before failing. Use LeaderRetryWait to control the wait time
	// between retries.
	//
	// Defaults to 10.
	LeaderRetryLimit int

	// LeaderRetryWait sets a limit to the waiting time when trying to connect
	// to a single node after failure.
	//
	// Defaults to 500ms.
	//
	// Timeout on a connection is controlled by the DialTimeout setting.
	LeaderRetryWait time.Duration

	// Any new connection dial timeout.
	//
	// Default is 10 seconds.
	DialTimeout time.Duration

	// Logger used by the broker.
	//
	// By default all messages are dropped.
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

// Broker is an abstract connection to kafka cluster, managing connections to
// all kafka nodes.
type Broker struct {
	conf BrokerConf

	mu       sync.Mutex
	metadata clusterMetadata
	conns    map[int32]*connection
	rand     *rand.Rand
}

// Dial connects to any node from a given list of kafka addresses and after
// successful metadata fetch, returns broker.
//
// The returned broker is not initially connected to any kafka node.
func Dial(nodeAddresses []string, conf BrokerConf) (*Broker, error) {
	broker := &Broker{
		conf:  conf,
		conns: make(map[int32]*connection),
		rand:  rand.New(rand.NewSource(time.Now().UTC().UnixNano())),
	}

	for _, addr := range nodeAddresses {
		conn, err := newConnection(addr, conf.DialTimeout)
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

// Close closes the broker and all active kafka nodes connections.
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
		conn, err := newConnection(addr, b.conf.DialTimeout)
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

	return nil, errors.New("cannot fetch metadata. No topics created?")
}

// rewriteMetadata creates new internal metadata representation using data from
// given response. It's call has to be protected with lock.
func (b *Broker) rewriteMetadata(resp *proto.MetadataResp) {
	if !b.metadata.created.IsZero() {
		b.conf.Log.Printf("rewriting metadata created %s ago", time.Now().Sub(b.metadata.created))
	}
	b.metadata = clusterMetadata{
		created:        time.Now(),
		nodes:          make(map[int32]string),
		endpoints:      make(map[string]int32),
		partitionCount: make(map[string]int32),
	}
	for _, node := range resp.Brokers {
		b.metadata.nodes[node.NodeID] = fmt.Sprintf("%s:%d", node.Host, node.Port)
	}
	for _, topic := range resp.Topics {
		b.metadata.partitionCount[topic.Name] = -1
		for _, part := range topic.Partitions {
			// Is there a better way to get the 'total number' of partitions in a topic? Also,
			// are gaps possible? Do we need to track every partition?
			if part.ID > b.metadata.partitionCount[topic.Name] {
				b.metadata.partitionCount[topic.Name] = part.ID
			}
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
			conn, err = newConnection(addr, b.conf.DialTimeout)
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

// coordinatorConnection returns connection to offset coordinator for given group.
//
// Failed connection retry is controlled by broker configuration.
func (b *Broker) coordinatorConnection(consumerGroup string) (conn *connection, err error) {
	// TODO(husio) clean this up
	for retry := 0; retry < b.conf.LeaderRetryLimit; retry++ {
		b.mu.Lock()

		// first try all aready existing connections
		for _, conn := range b.conns {
			resp, err := conn.ConsumerMetadata(&proto.ConsumerMetadataReq{
				ClientID:      b.conf.ClientID,
				ConsumerGroup: consumerGroup,
			})
			if err != nil {
				b.conf.Log.Printf("coordinator for %q metadata fetch error: %s", consumerGroup, err)
				continue
			}
			if resp.Err != nil {
				b.mu.Unlock()
				return nil, resp.Err
			}

			addr := fmt.Sprintf("%s:%d", resp.CoordinatorHost, resp.CoordinatorPort)
			conn, err := newConnection(addr, b.conf.DialTimeout)
			if err != nil {
				b.conf.Log.Printf("cannot connect to node %d (%s): %s", resp.CoordinatorID, addr, err)
				continue
			}
			b.conns[resp.CoordinatorID] = conn
			b.mu.Unlock()
			return conn, nil
		}

		// if none of the connections worked out, try with fresh data
		if err := b.refreshMetadata(); err != nil {
			return nil, err
		}

		for nodeID, addr := range b.metadata.nodes {
			if _, ok := b.conns[nodeID]; ok {
				// connection to node is cached so it was already checked
				continue
			}
			conn, err := newConnection(addr, b.conf.DialTimeout)
			if err != nil {
				b.conf.Log.Printf("cannot connect to node %d (%s): %s", nodeID, addr, err)
				continue
			}
			b.conns[nodeID] = conn

			resp, err := conn.ConsumerMetadata(&proto.ConsumerMetadataReq{
				ClientID:      b.conf.ClientID,
				ConsumerGroup: consumerGroup,
			})
			if err != nil {
				b.conf.Log.Printf("coordinator for %q metadata fetch error: %s", consumerGroup, err)
				continue
			}
			if resp.Err != nil {
				b.mu.Unlock()
				return nil, resp.Err
			}

			addr := fmt.Sprintf("%s:%d", resp.CoordinatorHost, resp.CoordinatorPort)
			conn, err = newConnection(addr, b.conf.DialTimeout)
			if err != nil {
				b.conf.Log.Printf("cannot connect to node %d (%s): %s", resp.CoordinatorID, addr, err)
				continue
			}
			b.conns[resp.CoordinatorID] = conn
			b.mu.Unlock()
			return conn, nil
		}
		b.mu.Unlock()

		time.Sleep(b.conf.LeaderRetryWait)
	}
	return nil, fmt.Errorf("coordinator for %q not found", consumerGroup)
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
			b.conf.Log.Printf("closing dead connection to %d", nid)
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

// OffsetEarliest returns the oldest offset available on the given partition.
func (b *Broker) OffsetEarliest(topic string, partition int32) (offset int64, err error) {
	return b.offset(topic, partition, -2)
}

// OffsetLatest return the offset of the next message produced in given partition
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

// NewProducerConf returns a default producer configuration.
func NewProducerConf() ProducerConf {
	return ProducerConf{
		RequestTimeout: time.Second * 5,
		RequiredAcks:   proto.RequiredAcksAll,
		RetryLimit:     5,
		RetryWait:      time.Millisecond * 200,
		Log:            nil,
	}
}

// producer is the link to the client with extra configuration.
type producer struct {
	conf   ProducerConf
	broker *Broker
}

// Producer returns new producer instance, bound to the broker.
func (b *Broker) Producer(conf ProducerConf) Producer {
	if conf.Log == nil {
		conf.Log = b.conf.Log
	}
	return &producer{
		conf:   conf,
		broker: b,
	}
}

// RandomPartition returns a random partition for a given topic..
func (b *Broker) RandomPartition(topic string, messages ...*proto.Message) int {
	if partitionCount, _ := b.metadata.partitionCount[topic]; partitionCount > 0 {
		return b.rand.Intn(int(partitionCount))
	}
	b.conf.Log.Printf("failed to select random partition for topic %s", topic)
	return 0
}

// Produce writes messages to the given destination. Writes within the call are
// atomic, meaning either all or none of them are written to kafka.  Produce
// has a configurable amount of retries which may be attempted when common
// errors are encountered.  This behaviour can be configured with the
// RetryLimit and RetryWait attributes.
//
// Upon a successful call, the message's Offset field is updated.
func (p *producer) Produce(topic string, partitionOrFunc interface{}, messages ...*proto.Message) (offset int64, err error) {
	// User can give either 'int' or 'Partitioner' interface.
	partition, ok := partitionOrFunc.(int)
	if !ok {
		// Wish Go would let me use the Partitioner definition for this type assertion...
		partitionFunc, ok := partitionOrFunc.(func (string, ...*proto.Message) int)
		if ok {
			partition = partitionFunc(topic, messages...)
		} else {
			return 0, errors.New(fmt.Sprintf("invalid partitionOrFunc type: %T", partitionOrFunc))
		}
	}

retryLoop:
	for retry := 0; retry < p.conf.RetryLimit; retry++ {
		offset, err = p.produce(topic, int32(partition), messages...)
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
func (p *producer) produce(topic string, partition int32, messages ...*proto.Message) (offset int64, err error) {
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
			p.conf.Log.Printf("connection to %s:%d died while sendnig message", topic, partition)
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

			// Don't overwrite err if it was already set
			if err == nil {
				err = part.Err
			}
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

	// RetryLimit limits fetching messages a given amount of times before
	// returning ErrNoData error.
	//
	// Default is -1, which turns this limit off.
	RetryLimit int

	// RetryWait controls the duration of wait between fetch request calls,
	// when no data was returned.
	//
	// Default is 50ms.
	RetryWait time.Duration

	// RetryErrLimit limits the number of retry attempts when an error is
	// encountered.
	//
	// Default is 10.
	RetryErrLimit int

	// RetryErrWait controls the wait duration between retries after failed
	// fetch request.
	//
	// Default is 500ms.
	RetryErrWait time.Duration

	// MinFetchSize is the minimum size of messages to fetch in bytes.
	//
	// Default is 1 to fetch any message available.
	MinFetchSize int32

	// MaxFetchSize is the maximum size of data which can be sent by kafka node
	// to consumer.
	//
	// Default is 2000000 bytes.
	MaxFetchSize int32

	// Consumer cursor starting point. Set to StartOffsetNewest to receive only
	// newly created messages or StartOffsetOldest to read everything. Assign
	// any offset value to manually set cursor -- consuming starts with the
	// message whose offset is equal to given value (including first message).
	//
	// Default is StartOffsetOldest.
	StartOffset int64

	// Logger used by consumer. By default, reuse logger assigned to broker.
	Log Logger
}

// NewConsumerConf returns the default consumer configuration.
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

// Consumer represents a single partition reading buffer. Consumer is also
// providing limited failure handling and message filtering.
type consumer struct {
	broker *Broker
	conn   *connection
	conf   ConsumerConf
	offset int64 // offset of next NOT consumed message

	mu     sync.Mutex
	msgbuf []*proto.Message
}

// Consumer creates a new consumer instance, bound to the broker.
func (b *Broker) Consumer(conf ConsumerConf) (Consumer, error) {
	conn, err := b.leaderConnection(conf.Topic, conf.Partition)
	if err != nil {
		return nil, err
	}
	if conf.Log == nil {
		conf.Log = b.conf.Log
	}
	offset := conf.StartOffset
	if offset < 0 {
		switch offset {
		case StartOffsetNewest:
			off, err := b.OffsetLatest(conf.Topic, conf.Partition)
			if err != nil {
				return nil, err
			}
			offset = off
		case StartOffsetOldest:
			off, err := b.OffsetEarliest(conf.Topic, conf.Partition)
			if err != nil {
				return nil, err
			}
			offset = off
		default:
			return nil, fmt.Errorf("invalid start offset: %d", conf.StartOffset)
		}
	}
	c := &consumer{
		broker: b,
		conn:   conn,
		conf:   conf,
		msgbuf: make([]*proto.Message, 0),
		offset: offset,
	}
	return c, nil
}

// Consume is returning single message from consumed partition. Consumer can
// retry fetching messages even if responses return no new data. Retry
// behaviour can be configured through RetryLimit and RetryWait consumer
// parameters.
//
// Consume can retry sending request on common errors. This behaviour can be
// configured with RetryErrLimit and RetryErrWait consumer configuration
// attributes.
func (c *consumer) Consume() (*proto.Message, error) {
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

		if len(messages) == toSkip && toSkip != 0 {
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
	c.offset = msg.Offset + 1
	return msg, nil
}

// fetch and return next batch of messages. In case of certain set of errors,
// retry sending fetch request. Retry behaviour can be configured with
// RetryErrLimit and RetryErrWait consumer configuration attributes.
func (c *consumer) fetch() ([]*proto.Message, error) {
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
						FetchOffset: c.offset,
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
			c.conf.Log.Printf("connection to %s:%d died while fetching message", c.conf.Topic, c.conf.Partition)
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
				return part.Messages, part.Err
			}
		}
		return nil, errors.New("incomplete fetch response")
	}
}

type OffsetCoordinatorConf struct {
	ConsumerGroup string

	// RetryErrLimit limits messages fetch retry upon failure. By default 10.
	RetryErrLimit int

	// RetryErrWait controls wait duration between retries after failed fetch
	// request. By default 500ms.
	RetryErrWait time.Duration

	// Logger used by consumer. By default, reuse logger assigned to broker.
	Log Logger
}

// NewOffsetCoordinatorConf returns default OffsetCoordinator configuration.
func NewOffsetCoordinatorConf(consumerGroup string) OffsetCoordinatorConf {
	return OffsetCoordinatorConf{
		ConsumerGroup: consumerGroup,
		RetryErrLimit: 10,
		RetryErrWait:  time.Millisecond * 500,
		Log:           nil,
	}
}

type offsetCoordinator struct {
	conf   OffsetCoordinatorConf
	broker *Broker
	conn   *connection
}

// OffsetCoordinator returns offset management coordinator for single consumer
// group, bound to broker.
func (b *Broker) OffsetCoordinator(conf OffsetCoordinatorConf) (OffsetCoordinator, error) {
	conn, err := b.coordinatorConnection(conf.ConsumerGroup)
	if err != nil {
		return nil, err
	}
	if conf.Log == nil {
		conf.Log = b.conf.Log
	}
	c := &offsetCoordinator{
		broker: b,
		conf:   conf,
		conn:   conn,
	}
	return c, nil
}

// Commit is saving offset information for given topic and partition.
//
// Commit can retry saving offset information on common errors. This behaviour
// can be configured with with RetryErrLimit and RetryErrWait coordinator
// configuration attributes.
func (c *offsetCoordinator) Commit(topic string, partition int32, offset int64) error {
	return c.commit(topic, partition, offset, "")
}

// Commit works exactly like Commit method, but store extra metadata string
// together with offset information.
func (c *offsetCoordinator) CommitFull(topic string, partition int32, offset int64, metadata string) error {
	return c.commit(topic, partition, offset, metadata)
}

// commit is saving offset and metadata information. Provides limited error
// handling configurable through OffsetCoordinatorConf.
func (c *offsetCoordinator) commit(topic string, partition int32, offset int64, metadata string) (err error) {
	var resp *proto.OffsetCommitResp

retryLoop:
	for retry := 0; retry < c.conf.RetryErrLimit; retry++ {
		// connection can be set to nil if previously reference connection died
		if c.conn == nil {
			conn, err := c.broker.coordinatorConnection(c.conf.ConsumerGroup)
			if err != nil {
				if retry == c.conf.RetryErrLimit {
					c.conf.Log.Printf("cannot connect to coordinator of %q consumer group: %s", c.conf.ConsumerGroup, err)
					return err
				}
				time.Sleep(c.conf.RetryErrWait)
				continue
			}
			c.conn = conn
		}

		resp, err = c.conn.OffsetCommit(&proto.OffsetCommitReq{
			ClientID:      c.broker.conf.ClientID,
			ConsumerGroup: c.conf.ConsumerGroup,
			Topics: []proto.OffsetCommitReqTopic{
				proto.OffsetCommitReqTopic{
					Name: topic,
					Partitions: []proto.OffsetCommitReqPartition{
						proto.OffsetCommitReqPartition{ID: partition, Offset: offset, TimeStamp: time.Now(), Metadata: metadata},
					},
				},
			},
		})

		switch err {
		case io.EOF, syscall.EPIPE:
			c.conf.Log.Printf("connection to %s:%d died while commiting %q offset", topic, partition, c.conf.ConsumerGroup)
			c.broker.closeDeadConnection(c.conn)
			c.conn = nil
			continue retryLoop
		case nil:
			// all good
			break retryLoop
		default:
			// we cannot help if the error is unknown
			break retryLoop
		}
	}

	if err != nil {
		c.conf.Log.Printf("cannot commit %s:%d offset for %q: %s", topic, partition, c.conf.ConsumerGroup, err)
		return err
	}

	for _, t := range resp.Topics {
		if t.Name != topic {
			c.conf.Log.Printf("unexpected topic information received: %s (expecting %s)", t.Name, topic)
			continue

		}
		for _, part := range t.Partitions {
			if part.ID != partition {
				c.conf.Log.Printf("unexpected partition information received: %s:%d", topic, part.ID)
				continue
			}
			return part.Err
		}
	}
	return errors.New("response does not contain commit information")
}

// Offset is returning last offset and metadata information committed for given
// topic and partition.
// Offset can retry sending request on common errors. This behaviour can be
// configured with with RetryErrLimit and RetryErrWait coordinator
// configuration attributes.
func (c *offsetCoordinator) Offset(topic string, partition int32) (offset int64, metadata string, err error) {
	var resp *proto.OffsetFetchResp

retryLoop:
	for retry := 0; retry < c.conf.RetryErrLimit; retry++ {
		// connection can be set to nil if previously reference connection died
		if c.conn == nil {
			conn, err := c.broker.coordinatorConnection(c.conf.ConsumerGroup)
			if err != nil {
				if retry == c.conf.RetryErrLimit {
					c.conf.Log.Printf("cannot connect to coordinator of %q consumer group: %s", c.conf.ConsumerGroup, err)
					return 0, "", err
				}
				time.Sleep(c.conf.RetryErrWait)
				continue
			}
			c.conn = conn
		}
		resp, err = c.conn.OffsetFetch(&proto.OffsetFetchReq{
			ConsumerGroup: c.conf.ConsumerGroup,
			Topics: []proto.OffsetFetchReqTopic{
				proto.OffsetFetchReqTopic{
					Name:       topic,
					Partitions: []int32{partition},
				},
			},
		})

		switch err {
		case io.EOF, syscall.EPIPE:
			c.conf.Log.Printf("connection to %s:%d died while fetching %q offset", topic, partition, c.conf.ConsumerGroup)
			c.broker.closeDeadConnection(c.conn)
			c.conn = nil
			continue retryLoop
		case nil:
			// all good
			break retryLoop
		default:
			// we cannot help if the error is unknown
			break retryLoop
		}
	}

	if err != nil {
		c.conf.Log.Printf("cannot fetch %s:%d offset for %q: %s", topic, partition, c.conf.ConsumerGroup, err)
		return 0, "", err
	}

	for _, t := range resp.Topics {
		if t.Name != topic {
			c.conf.Log.Printf("unexpected topic information received: %s (expecting %s)", t.Name, topic)
			continue

		}
		for _, part := range t.Partitions {
			if part.ID != partition {
				c.conf.Log.Printf("unexpected partition information received: %s:%d", topic, part.ID)
				continue
			}
			if part.Err != nil {
				return 0, "", part.Err
			}
			return part.Offset, part.Metadata, nil
		}
	}
	return 0, "", errors.New("response does not contain offset information")
}
