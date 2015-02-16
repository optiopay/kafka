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
	Nodes     map[int32]string // node ID to address
	Endpoints map[string]int32 // topic:partition to leader node ID
}

type BrokerConfig struct {
	// Kafka client ID.
	ClientID string

	// Any new connection dial timeout. By default 10 seconds.
	DialTimeout time.Duration

	// Logger used by the broker. By default all messages are dropped.
	Log Logger
}

func NewBrokerConfig(clientID string) BrokerConfig {
	return BrokerConfig{
		ClientID:    clientID,
		DialTimeout: time.Second * 10,
		Log:         log.New(ioutil.Discard, "kafka", log.LstdFlags),
	}
}

// Broker is abstrac connection to kafka cluster, managing connections to all
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
		metadata: clusterMetadata{
			Nodes:     make(map[int32]string),
			Endpoints: make(map[string]int32),
		},
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
		for _, node := range resp.Brokers {
			broker.metadata.Nodes[node.NodeID] = fmt.Sprintf("%s:%d", node.Host, node.Port)
		}
		for _, topic := range resp.Topics {
			for _, part := range topic.Partitions {
				broker.metadata.Endpoints[fmt.Sprintf("%s:%d", topic.Name, part.ID)] = part.Leader
			}
		}
		return broker, nil
	}
	return nil, errors.New("could not connect")
}

func (b *Broker) Close() error {
	// TODO(husio)
	return nil
}

// leaderConnection returns connection to leader for given partition. If
// connection does not exist, broker will try to connect first and add store
// connection for any further use.
func (b *Broker) leaderConnection(topic string, partition int32) (conn *connection, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	endpoint := fmt.Sprintf("%s:%d", topic, partition)
	nodeID, ok := b.metadata.Endpoints[endpoint]
	if !ok {
		// TODO(husio) refresh metadata and check again
		return nil, proto.ErrUnknownTopicOrPartition
	}
	conn, ok = b.conns[nodeID]
	if !ok {
		addr, ok := b.metadata.Nodes[nodeID]
		if !ok {
			return nil, proto.ErrBrokerNotAvailable
		}
		conn, err = NewConnection(addr, b.config.DialTimeout)
		if err != nil {
			return nil, err
		}
		b.conns[nodeID] = conn
	}
	return conn, nil
}

func (b *Broker) offset(topic string, partition int32, timems int64) (offset int64, err error) {
	conn, err := b.leaderConnection(topic, partition)
	if err != nil {
		return -1, err
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
		return -1, err
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
		return -1, errors.New("incomplete fetch response")
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
	// servers to write, proto.RequiredAcksLocal to wait only for leater node
	// answer or proto.RequiredAcksNone to not wait for any response.
	// Setting this to any other, greater than zero value will make producer to
	// wait for given number of servers to confirm write before returning.
	RequiredAcks int16

	// Logger used by producer. By default, reuse logger assigned to broker.
	Log Logger
}

// NewProducerConfig return default producer configuration
func NewProducerConfig() ProducerConfig {
	return ProducerConfig{
		RequestTimeout: time.Second * 5,
		RequiredAcks:   proto.RequiredAcksAll,
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
func (p *Producer) Produce(topic string, partition int32, messages ...*Message) (offset int64, err error) {
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
		// TODO(husio) handle some of the errors
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
