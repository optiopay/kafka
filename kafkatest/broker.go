package kafkatest

import (
	"errors"
	"github.com/husio/kafka"
	"github.com/husio/kafka/proto"
	"sync"
	"time"
)

var (
	ErrTimeout = errors.New("timeout")

	// test implementation should implement the interface
	_ kafka.Client  = &Broker{}
	_ kafka.Sender  = &Producer{}
	_ kafka.Fetcher = &Consumer{}
)

// Broker is mock version of kafka's broker. It's implementing Broker interface
// and provides easy way of mocking server actions.
type Broker struct {
	produced chan *ProducedMessages

	mu        sync.Mutex
	consumers map[string]map[int32]*Consumer

	// OffsetEarliestHandler is callback function called whenever
	// OffsetEarliest method of the broker is called. Overwrite to change
	// default behaviour -- always returning ErrUnknownTopicOrPartition
	OffsetEarliestHandler func(string, int32) (int64, error)

	// OffsetLatestHandler is callback function called whenever OffsetLatest
	// method of the broker is called. Overwrite to change default behaviour --
	// always returning ErrUnknownTopicOrPartition
	OffsetLatestHandler func(string, int32) (int64, error)
}

func NewBroker() *Broker {
	return &Broker{
		consumers: make(map[string]map[int32]*Consumer),
		produced:  make(chan *ProducedMessages),
	}
}

// Close is no operation method, required by Broker interface.
func (b *Broker) Close() {
}

// OffsetEarliest return result of OffsetEarliestHandler callback set on the
// broker. If not set, always return ErrUnknownTopicOrPartition
func (b *Broker) OffsetEarliest(topic string, partition int32) (int64, error) {
	if b.OffsetEarliestHandler != nil {
		return b.OffsetEarliestHandler(topic, partition)
	}
	return 0, proto.ErrUnknownTopicOrPartition
}

// OffsetLatest return result of OffsetLatestHandler callback set on the
// broker. If not set, always return ErrUnknownTopicOrPartition
func (b *Broker) OffsetLatest(topic string, partition int32) (int64, error) {
	if b.OffsetLatestHandler != nil {
		return b.OffsetLatestHandler(topic, partition)
	}
	return 0, proto.ErrUnknownTopicOrPartition
}

// Consumer returns consumer mock and never error.
//
// At most one consumer for every topic-partition pair can be created --
// calling this for the same topic-partition will always return the same
// consumer instance.
func (b *Broker) Consumer(conf kafka.ConsumerConf) (kafka.Fetcher, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if t, ok := b.consumers[conf.Topic]; ok {
		if c, ok := t[conf.Partition]; ok {
			return c, nil
		}
	} else {
		b.consumers[conf.Topic] = make(map[int32]*Consumer)
	}

	c := &Consumer{
		conf:     conf,
		Broker:   b,
		Messages: make(chan *proto.Message),
		Errors:   make(chan error),
	}
	b.consumers[conf.Topic][conf.Partition] = c
	return c, nil
}

// Producer returns producer mock instance.
func (b *Broker) Producer(kafka.ProducerConf) kafka.Sender {
	return &Producer{
		Broker:         b,
		ResponseOffset: 1,
	}
}

// ReadProducers return ProduceMessages representing produce call of one of
// created by broker producers or ErrTimeout.
func (b *Broker) ReadProducers(timeout time.Duration) (*ProducedMessages, error) {
	select {
	case p := <-b.produced:
		return p, nil
	case <-time.After(timeout):
		return nil, ErrTimeout
	}
}

// Consumer mocks kafka's consumer. Use Messages and Errors channels to mock
// Fetch method results.
type Consumer struct {
	conf kafka.ConsumerConf

	Broker *Broker

	// Messages is channel consumed by fetch method call. Pushing message into
	// this channel will result in Fetch method call returning message data.
	Messages chan *proto.Message

	// Errors is channel consumed by fetch method call. Pushing error into this
	// channel will result in Fetch method call returning error.
	Errors chan error
}

// Fetch returns message or error pushed through consumers Messages and Errors
// channel. Function call will block until data on at least one of those
// channels is available.
func (c *Consumer) Fetch() (*proto.Message, error) {
	select {
	case msg := <-c.Messages:
		msg.Topic = c.conf.Topic
		msg.Partition = c.conf.Partition
		return msg, nil
	case err := <-c.Errors:
		return nil, err
	}
}

// Producer mocks kafka's producer.
type Producer struct {
	Broker *Broker

	// ResponseOffset is offset counter returned and incremented by every
	// Produce method call. By default set to 1.
	ResponseOffset int64

	// ResponseError if set, force Produce method call to instantly return
	// error, without publishing messages. By default nil.
	ResponseError error
}

// ProducedMessages represents all arguments used for single Produce method
// call.
type ProducedMessages struct {
	Topic     string
	Partition int32
	Messages  []*proto.Message
}

// Produce is settings messages Crc and Offset attributes and pushing all
// passed arguments to broker. Produce call is blocking until pushed message
// will be read with broker's ReadProduces.
func (p *Producer) Produce(topic string, partition int32, messages ...*proto.Message) (int64, error) {
	if p.ResponseError != nil {
		return 0, p.ResponseError
	}
	off := p.ResponseOffset

	for i, msg := range messages {
		msg.Offset = off + int64(i)
		msg.Crc = proto.ComputeCrc(msg)
	}

	p.Broker.produced <- &ProducedMessages{
		Topic:     topic,
		Partition: partition,
		Messages:  messages,
	}
	p.ResponseOffset += int64(len(messages))
	return off, nil
}
