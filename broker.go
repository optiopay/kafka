package kafka

import (
	"errors"
	"fmt"
	"log"
	"math"
	"sync"
	"time"
)

const megabyte = 1048576

var (
	// Returned by consumer Fetch when retry limit was set and exceeded during
	// single function call
	ErrNoData = errors.New("no data")
)

type clusterMetadata struct {
	Nodes     map[int32]string // node ID to address
	Endpoints map[string]int32 // topic:partition to leader node ID
}

type BrokerConfig struct {
	ClientID string
}

type Broker struct {
	config BrokerConfig

	mu       sync.Mutex
	metadata clusterMetadata
	conns    map[int32]*connection
}

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
		conn, err := newConnection(addr)
		if err != nil {
			log.Printf("could not connect to %s: %s", addr, err)
			continue
		}
		resp, err := conn.Metadata(&MetadataReq{
			ClientID: broker.config.ClientID,
			Topics:   nil,
		})
		if err != nil {
			log.Printf("could not fetch metadata from %s: %s", addr, err)
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

func (b *Broker) leaderConnection(topic string, partition int32) (conn *connection, err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	endpoint := fmt.Sprintf("%s:%d", topic, partition)
	nodeID, ok := b.metadata.Endpoints[endpoint]
	if !ok {
		// TODO(husio) refresh metadata and check again
		return nil, ErrUnknownTopicOrPartition
	}
	conn, ok = b.conns[nodeID]
	if !ok {
		addr, ok := b.metadata.Nodes[nodeID]
		if !ok {
			return nil, ErrBrokerNotAvailable
		}
		conn, err = newConnection(addr)
		if err != nil {
			return nil, err
		}
		b.conns[nodeID] = conn
	}
	return conn, nil
}

type ProducerConfig struct {
	Timeout      time.Duration
	RequiredAcks int16
}

// NewProducerConfig return default producer configuration
func NewProducerConfig() ProducerConfig {
	return ProducerConfig{
		Timeout:      time.Second,
		RequiredAcks: RequiredAcksAll,
	}
}

type Producer struct {
	// TODO(husio) configuration
	config ProducerConfig
	broker *Broker
}

func (b *Broker) Producer(config ProducerConfig) *Producer {
	return &Producer{
		config: config,
		broker: b,
	}
}

func (p *Producer) Config() ProducerConfig {
	return p.config
}

func (p *Producer) Produce(topic string, partition int32, messages ...*Message) (offset int64, err error) {
	conn, err := p.broker.leaderConnection(topic, partition)
	if err != nil {
		return 0, err
	}
	req := ProduceReq{
		ClientID:     p.broker.config.ClientID,
		RequiredAcks: p.config.RequiredAcks,
		Timeout:      p.config.Timeout,
		Topics: []ProduceReqTopic{
			ProduceReqTopic{
				Name: topic,
				Partitions: []ProduceReqPartition{
					ProduceReqPartition{
						Partition: partition,
						Messages:  messages,
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
			log.Printf("unexpected topic information received: %s", t.Name)
			continue
		}
		for _, part := range t.Partitions {
			if part.Partition != partition {
				log.Printf("unexpected partition information received: %s:%d", t.Name, part.Partition)
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
	Topic     string
	Partition int32
	// FetchTimeout controlls fetch request timeout. This operation is blocking
	// the whole connection, so it should always be set to small value. By
	// default it's set to 0.
	FetchTimeout time.Duration
	// FetchRetryLimit limits fetching messages given amount of times before
	// returning ErrNoData error. By default set to -1, which turns this limit
	// off.
	FetchRetryLimit int
	// MinFetchSize is minimum size of messages to fetch in bytes. By default
	// set to 1 to fetch any message available.
	MinFetchSize int32
	MaxFetchSize int32
}

// NewConsumerConfig return default consumer configuration
func NewConsumerConfig(topic string, partition int32) ConsumerConfig {
	return ConsumerConfig{
		Topic:           topic,
		Partition:       partition,
		FetchTimeout:    0,
		FetchRetryLimit: -1,
		MinFetchSize:    1,
		MaxFetchSize:    megabyte * 2,
	}
}

type Consumer struct {
	broker *Broker
	conn   *connection
	config ConsumerConfig
	offset int64
	msgbuf []*Message
}

func (b *Broker) Consumer(config ConsumerConfig) (consumer *Consumer, err error) {
	conn, err := b.leaderConnection(config.Topic, config.Partition)
	if err != nil {
		return nil, err
	}
	consumer = &Consumer{
		broker: b,
		conn:   conn,
		config: config,
		msgbuf: make([]*Message, 0),
		offset: 0,
	}
	return consumer, nil
}

func (c *Consumer) Config() ConsumerConfig {
	return c.config
}

func (c *Consumer) Fetch() (*Message, error) {
	var retry int

	for len(c.msgbuf) == 0 {
		req := FetchReq{
			ClientID:    c.broker.config.ClientID,
			MaxWaitTime: c.config.FetchTimeout,
			MinBytes:    c.config.MinFetchSize,
			Sources: []FetchReqTopic{
				FetchReqTopic{
					Topic: c.config.Topic,
					Partitions: []FetchReqPartition{
						FetchReqPartition{
							Partition:   c.config.Partition,
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
		for _, topic := range resp.Sources {
			if topic.Topic != c.config.Topic {
				log.Printf("unexpected topic information received: %s (expecting %s)", topic.Topic)
				continue
			}
			for _, part := range topic.Partitions {
				if part.Partition != c.config.Partition {
					log.Printf("unexpected partition information received: %s:%d", topic.Topic, part.Partition)
					continue
				}

				found = true
				if len(part.Messages) == 0 {
					time.Sleep(time.Duration(math.Log(float64(retry+2))*250) * time.Millisecond)
					retry += 1
					if c.config.FetchRetryLimit != -1 && retry > c.config.FetchRetryLimit {
						return nil, ErrNoData
					}
				} else {
					last := part.Messages[len(part.Messages)-1]
					c.offset = last.Offset
					c.msgbuf = part.Messages
				}
			}
		}
		if !found {
			return nil, errors.New("incomplete fetch response")
		}
	}

	msg := c.msgbuf[0]
	c.msgbuf = c.msgbuf[1:]
	return msg, nil
}
