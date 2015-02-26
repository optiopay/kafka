package kafka

import (
	"fmt"
	"log"
	"os"

	"github.com/husio/kafka/proto"
)

func ExampleConsumer() {
	addresses := []string{"localhost:9092", "localhost:9093"}
	broker, err := Dial(addresses, NewBrokerConf("test"))
	if err != nil {
		panic(err)
	}
	defer broker.Close()

	conf := NewConsumerConf("my-messages", 0)
	conf.RetryLimit = 20
	conf.Log = log.New(os.Stderr, "[consumer:my-messages]", log.LstdFlags)
	conf.StartOffset = StartOffsetNewest

	consumer, err := broker.Consumer(conf)
	if err != nil {
		panic(err)
	}

	for {
		msg, err := consumer.Fetch()
		if err != nil {
			if err == ErrNoData {
				break
			}
			panic(err)
		}

		fmt.Printf("message: %#v", msg)
	}
}

func ExampleProducer() {
	addresses := []string{"localhost:9092", "localhost:9093"}
	broker, err := Dial(addresses, NewBrokerConf("test"))
	if err != nil {
		panic(err)
	}
	defer broker.Close()

	conf := NewProducerConf()
	conf.Log = log.New(os.Stderr, "[producer]", log.LstdFlags)
	conf.RequiredAcks = proto.RequiredAcksLocal

	producer := broker.Producer(conf)
	messages := []*proto.Message{
		&proto.Message{Value: []byte("first")},
		&proto.Message{Value: []byte("second")},
	}
	if _, err := producer.Produce("my-messages", 0, messages...); err != nil {
		panic(err)
	}
}

func ExampleMultiplexer() {
	addresses := []string{"localhost:9092", "localhost:9093"}
	broker, err := Dial(addresses, NewBrokerConf("test"))
	if err != nil {
		panic(err)
	}
	defer broker.Close()

	topics := []string{"fruits", "vegetables"}
	fetchers := make([]Fetcher, len(topics))

	for i, topic := range topics {
		conf := NewConsumerConf(topic, 0)
		conf.RetryLimit = 20
		conf.Log = log.New(os.Stderr, fmt.Sprintf("[consumer:%s]", topic), log.LstdFlags)
		conf.StartOffset = StartOffsetNewest
		consumer, err := broker.Consumer(conf)
		if err != nil {
			panic(err)
		}
		fetchers[i] = consumer
	}

	mx := Merge(fetchers...)
	defer mx.Close()

	for {
		msg, err := mx.Fetch()
		if err != nil {
			panic(err)
		}
		fmt.Printf("message: %#v", msg)
	}
}
