package kafka

import (
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"sync"
	"time"

	"github.com/optiopay/kafka/proto"
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
)

type Logger interface {
	Print(...interface{})
	Printf(string, ...interface{})
}

type clusterMetadata struct {
	created   time.Time
	nodes     map[int32]string // node ID to address
	endpoints map[string]int32 // topic:partition to leader node ID
}

type BrokerConfig struct {
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

func NewBrokerConfig(clientID string) BrokerConfig {
	return BrokerConfig{
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
	config BrokerConfig

	mu       sync.Mutex
	metadata clusterMetadata
	conns    map[int32]*connection
}

// Dial connects to any node from given list of kafka addresses and after
// successful metadata fetch, returns broker.
// Returned broker is not initially connected to any kafka node.
func Dial(nodeAddresses []string, config BrokerConfig) (*Broker, error) {
	broker := &Broker{
		config: config,
		conns:  make(map[int32]*connection),
	}

	for _, addr := range nodeAddresses {
		conn, err := NewConnection(addr, config.DialTimeout)
		if err != nil {
			config.Log.Printf("could not connect to %s: %s", addr, err)
			continue
		}
		defer conn.Close()
		resp, err := conn.Metadata(&proto.MetadataReq{
			ClientID: broker.config.ClientID,
			Topics:   nil,
		})
		if err != nil {
			config.Log.Printf("could not fetch metadata from %s: %s", addr, err)
			continue
		}
		broker.rewriteMetadata(resp)
		return broker, nil
	}
	return nil, errors.New("could not connect")
}

func (b *Broker) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for nodeID, conn := range b.conns {
		if err := conn.Close(); err != nil {
			b.config.Log.Printf("failed closing node %d connection: %s", nodeID, err)
		}
	}
}

// refreshMetadata is requesting metadata information from any node and refresh
// internal cached representation.
// Because it's changing internal state, this method requires lock protection,
// but it does not acquire nor release lock itself.
func (b *Broker) refreshMetadata() error {
	checkednodes := make(map[int32]bool)

	// try all existing connections first
	for nodeID, conn := range b.conns {
		checkednodes[nodeID] = true
		resp, err := conn.Metadata(&proto.MetadataReq{
			ClientID: b.config.ClientID,
			Topics:   nil,
		})
		if err != nil {
			b.config.Log.Printf("cannot fetch metadata from node %d: %s", nodeID, err)
			continue
		}
		b.rewriteMetadata(resp)
		return nil
	}

	// try all nodes that we know of that we're not connected to
	for nodeID, addr := range b.metadata.nodes {
		if _, ok := checkednodes[nodeID]; ok {
			continue
		}
		conn, err := NewConnection(addr, b.config.DialTimeout)
		if err != nil {
			b.config.Log.Printf("could not connect to %s: %s", addr, err)
			continue
		}
		// we had no active connection to this node, so most likely we don't need it
		defer conn.Close()

		resp, err := conn.Metadata(&proto.MetadataReq{
			ClientID: b.config.ClientID,
			Topics:   nil,
		})
		if err != nil {
			b.config.Log.Printf("cannot fetch metadata from node %d: %s", nodeID, err)
			continue
		}
		b.rewriteMetadata(resp)
		return nil
	}

	return errors.New("cannot fetch metadata")
}

// rewriteMetadata creates new internal metadata representation using data from
// given response. It's call has to be protected with lock.
func (b *Broker) rewriteMetadata(resp *proto.MetadataResp) {
	b.config.Log.Printf("rewriting metadata created %s ago", time.Now().Sub(b.metadata.created))
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

	for retry := 0; retry < b.config.LeaderRetryLimit; retry++ {
		b.mu.Lock()

		nodeID, ok := b.metadata.endpoints[endpoint]
		if !ok {
			b.refreshMetadata()
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
				b.mu.Unlock()
				return nil, proto.ErrBrokerNotAvailable
			}
			conn, err = NewConnection(addr, b.config.DialTimeout)
			if err != nil {
				b.config.Log.Printf("cannot connect to node %s: %s", addr, err)
				b.mu.Unlock()
				time.Sleep(b.config.LeaderRetryWait)
				continue
			}
			b.conns[nodeID] = conn
		}
		b.mu.Unlock()
		return conn, nil
	}
	return nil, err
}

func (b *Broker) offset(topic string, partition int32, timems int64) (offset int64, err error) {
	conn, err := b.leaderConnection(topic, partition)
	if err != nil {
		return 0, err
	}
	resp, err := conn.Offset(&proto.OffsetReq{
		ClientID:  b.config.ClientID,
		ReplicaID: -1,
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
			b.config.Log.Printf("unexpected topic information received: %s (expecting %s)", t.Name)
			continue
		}
		for _, part := range t.Partitions {
			if part.ID != partition {
				b.config.Log.Printf("unexpected partition information received: %s:%s (expecting %s)", t.Name, part.ID, partition)
				continue
			}
			found = true
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

type ProducerConfig struct {
	// Timeout of single produce request. By default, 5 seconds.
	RequestTimeout time.Duration

	// Message ACK configuration. Use proto.RequiredAcksAll to require all
	// servers to write, proto.RequiredAcksLocal to wait only for leader node
	// answer or proto.RequiredAcksNone to not wait for any response.
	// Setting this to any other, greater than zero value will make producer to
	// wait for given number of servers to confirm write before returning.
	RequiredAcks int16

	RetryLimit int
	RetryWait  time.Duration

	// Logger used by producer. By default, reuse logger assigned to broker.
	Log Logger
}

// NewProducerConfig return default producer configuration
func NewProducerConfig() ProducerConfig {
	return ProducerConfig{
		RequestTimeout: time.Second * 5,
		RequiredAcks:   proto.RequiredAcksAll,
		RetryLimit:     5,
		RetryWait:      time.Millisecond * 200,
		Log:            nil,
	}
}

// Producer is link to broker with extra configuration.
type Producer struct {
	config ProducerConfig
	broker *Broker
}

func (b *Broker) Producer(config ProducerConfig) *Producer {
	if config.Log == nil {
		config.Log = b.config.Log
	}
	return &Producer{
		config: config,
		broker: b,
	}
}

func (p *Producer) Config() ProducerConfig {
	return p.config
}

type Message struct {
	Key   []byte
	Value []byte
}

// Produce writes messages to given destination. Write within single Produce
// call are atomic, meaning either all or none of them are written to kafka.
// Produce can retry sending request on common errors. This behaviour can be
// configured with RetryLimit and RetryWait producer configuration attributes.
func (p *Producer) Produce(topic string, partition int32, messages ...*Message) (offset int64, err error) {
	for retry := 0; retry < p.config.RetryLimit; retry++ {
		offset, err = p.produce(topic, partition, messages...)
		switch err {
		case proto.ErrLeaderNotAvailable, proto.ErrBrokerNotAvailable:
			p.config.Log.Printf("failed to produce messages (%d): %s", retry, err)
			time.Sleep(p.config.RetryWait)
			// TODO(husio) possible thundering herd
			if err := p.broker.refreshMetadata(); err != nil {
				p.config.Log.Printf("failed to refresh metadata: %s", err)
			}
		case proto.ErrRequestTimeout:
			p.config.Log.Printf("failed to produce messages (%d): %s", retry, err)
			time.Sleep(p.config.RetryWait)
		default:
			break
		}
	}
	return offset, err
}

// produce send produce request to leader for given destination.
func (p *Producer) produce(topic string, partition int32, messages ...*Message) (offset int64, err error) {
	conn, err := p.broker.leaderConnection(topic, partition)
	if err != nil {
		return 0, err
	}

	msgs := make([]*proto.Message, len(messages))
	for i, m := range messages {
		msgs[i] = &proto.Message{
			Key:   m.Key,
			Value: m.Value,
		}
	}
	req := proto.ProduceReq{
		ClientID:     p.broker.config.ClientID,
		RequiredAcks: p.config.RequiredAcks,
		Timeout:      p.config.RequestTimeout,
		Topics: []proto.ProduceReqTopic{
			proto.ProduceReqTopic{
				Name: topic,
				Partitions: []proto.ProduceReqPartition{
					proto.ProduceReqPartition{
						ID:       partition,
						Messages: msgs,
					},
				},
			},
		},
	}

	resp, err := conn.Produce(&req)
	if err != nil {
		return 0, err
	}

	// we expect single partition response
	found := false
	for _, t := range resp.Topics {
		if t.Name != topic {
			p.config.Log.Printf("unexpected topic information received: %s", t.Name)
			continue
		}
		for _, part := range t.Partitions {
			if part.ID != partition {
				p.config.Log.Printf("unexpected partition information received: %s:%d", t.Name, part.ID)
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

type ConsumerConfig struct {
	// Topic name that should be consumed
	Topic string

	// Partition ID that should be consumed.
	Partition int32

	// RequestTimeout controlls fetch request timeout.This operation is
	// blocking the whole connection, so it should always be set to small
	// value. By default it's set to 0.
	// To control fetch function timeout use RetryLimit and RetryWait.
	RequestTimeout time.Duration

	// RetryLimit limits fetching messages given amount of times before
	// returning ErrNoData error. By default set to -1, which turns this limit
	// off.
	RetryLimit int

	// MinFetchSize is minimum size of messages to fetch in bytes. By default
	// set to 1 to fetch any message available.
	MinFetchSize int32
	MaxFetchSize int32

	// RetryWait controlls duration of wait between fetch request calls, when
	// no data was returned. Defaults to 250ms.
	RetryWait time.Duration

	// Consumer cursor starting point. Set to StartOffsetNewest to receive only
	// newly created messages or StartOffsetOldest to read everything. Assign
	// any offset value to manually set cursor. Defaults to StartOffsetOldest
	StartOffset int64

	// Logger used by consumer. By default, reuse logger assigned to broker.
	Log Logger
}

// NewConsumerConfig return default consumer configuration
func NewConsumerConfig(topic string, partition int32) ConsumerConfig {
	return ConsumerConfig{
		Topic:          topic,
		Partition:      partition,
		RequestTimeout: 0,
		RetryLimit:     -1,
		MinFetchSize:   1,
		MaxFetchSize:   2000000,
		RetryWait:      time.Millisecond * 250,
		StartOffset:    StartOffsetOldest,
		Log:            nil,
	}
}

// Consumer is representing single partition reading buffer.
type Consumer struct {
	broker *Broker
	conn   *connection
	config ConsumerConfig
	offset int64
	msgbuf []*proto.Message
}

// Consumer creates cursor capable of reading messages from single source.
func (b *Broker) Consumer(config ConsumerConfig) (consumer *Consumer, err error) {
	conn, err := b.leaderConnection(config.Topic, config.Partition)
	if err != nil {
		return nil, err
	}
	if config.Log == nil {
		config.Log = b.config.Log
	}
	offset := config.StartOffset
	if config.StartOffset < 0 {
		switch config.StartOffset {
		case StartOffsetNewest:
			off, err := b.OffsetLatest(config.Topic, config.Partition)
			if err != nil {
				return nil, err
			}
			offset = off - 1
		case StartOffsetOldest:
			off, err := b.OffsetEarliest(config.Topic, config.Partition)
			if err != nil {
				return nil, err
			}
			offset = off
		default:
			return nil, fmt.Errorf("invalid start offset: %d", config.StartOffset)
		}
	}
	consumer = &Consumer{
		broker: b,
		conn:   conn,
		config: config,
		msgbuf: make([]*proto.Message, 0),
		offset: offset,
	}
	return consumer, nil
}

func (c *Consumer) Config() ConsumerConfig {
	return c.config
}

// Fetch is returning single message from consumed partition.
func (c *Consumer) Fetch() (*proto.Message, error) {
	var retry int

	for len(c.msgbuf) == 0 {
		req := proto.FetchReq{
			ClientID:    c.broker.config.ClientID,
			MaxWaitTime: c.config.RequestTimeout,
			MinBytes:    c.config.MinFetchSize,
			Topics: []proto.FetchReqTopic{
				proto.FetchReqTopic{
					Name: c.config.Topic,
					Partitions: []proto.FetchReqPartition{
						proto.FetchReqPartition{
							ID:          c.config.Partition,
							FetchOffset: c.offset + 1,
							MaxBytes:    c.config.MaxFetchSize,
						},
					},
				},
			},
		}
		resp, err := c.conn.Fetch(&req)
		if err != nil {
			// TODO(husio) handle some of the errors
			return nil, err
		}

		found := false
		for _, topic := range resp.Topics {
			if topic.Name != c.config.Topic {
				c.config.Log.Printf("unexpected topic information received: %s (expecting %s)", topic.Name)
				continue
			}
			for _, part := range topic.Partitions {
				if part.ID != c.config.Partition {
					c.config.Log.Printf("unexpected partition information received: %s:%d", topic.Name, part.ID)
					continue
				}

				found = true
				if len(part.Messages) == 0 {
					time.Sleep(time.Duration(math.Log(float64(retry+2))) * c.config.RetryWait)
					retry += 1
					if c.config.RetryLimit != -1 && retry > c.config.RetryLimit {
						return nil, ErrNoData
					}
					continue
				}

				// check first messages if any of them has index lower than requested
				toSkip := 0
				messages := part.Messages
				for toSkip < len(messages) {
					if messages[toSkip].Offset >= c.offset {
						break
					}
					toSkip++
				}

				// ignore all messages that are of index lower than requested
				messages = messages[toSkip:]
				if len(messages) == 0 {
					found = false
					continue
				}

				c.msgbuf = messages
			}
		}
		if !found {
			return nil, errors.New("incomplete fetch response")
		}
	}

	msg := c.msgbuf[0]
	c.msgbuf = c.msgbuf[1:]
	c.offset = msg.Offset
	return msg, nil
}
