package kafka

import (
	"testing"
	"time"

	"github.com/optiopay/kafka/kafkatest"
	"github.com/optiopay/kafka/proto"
)

func TestDialWithInvalidAddress(t *testing.T) {
	srv := kafkatest.NewServer()
	srv.Start()
	defer srv.Close()

	addresses := []string{"localhost:4291190", "localhost:2141202", srv.Address()}
	config := NewBrokerConfig("tester")
	config.DialTimeout = time.Millisecond * 200
	broker, err := Dial(addresses, config)
	if err != nil {
		t.Fatalf("could not create broker: %s", err)
	}
	if err := broker.Close(); err != nil {
		t.Fatalf("could not close broker: %s", err)
	}
}

func TestProducer(t *testing.T) {
	srv := kafkatest.NewServer()
	srv.Start()
	defer srv.Close()

	srv.Logs.AddPartition("test", 0, 1, 2, 3)

	config := NewBrokerConfig("tester")
	config.DialTimeout = time.Millisecond * 200
	broker, err := Dial([]string{srv.Address()}, config)
	if err != nil {
		t.Fatalf("could not create broker: %s", err)
	}

	producer := broker.Producer(NewProducerConfig())
	messages := []*Message{
		&Message{Value: []byte("first")},
		&Message{Value: []byte("second")},
	}
	_, err = producer.Produce("does-not-exist", 42142, messages...)
	if err != proto.ErrUnknownTopicOrPartition {
		t.Fatalf("expected '%s', got %s", proto.ErrUnknownTopicOrPartition, err)
	}

	offset, err := producer.Produce("test", 2, messages...)
	if err != nil {
		t.Fatalf("expected no error, got %s", err)
	}
	if offset != 1 {
		t.Fatalf("expected offset different than %d", offset)
	}

	offset, err = producer.Produce("test", 2, &Message{Value: []byte("third")})
	if err != nil {
		t.Fatalf("expected no error, got %s", err)
	}
	if offset != 3 {
		t.Fatalf("expected offset different than %d", offset)
	}

	offset, err = producer.Produce("test", 0, &Message{Value: []byte("first")})
	if err != nil {
		t.Fatalf("expected no error, got %s", err)
	}
	if offset != 1 {
		t.Fatalf("expected offset different than %d", offset)
	}

	if err := broker.Close(); err != nil {
		t.Fatalf("could not close broker: %s", err)
	}
}

func TestConsumer(t *testing.T) {
	srv := kafkatest.NewServer()
	srv.Start()
	defer srv.Close()

	srv.Logs.AddPartition("test", 0, 1, 2, 3)

	brokConfig := NewBrokerConfig("tester")
	brokConfig.DialTimeout = time.Millisecond * 200
	broker, err := Dial([]string{srv.Address()}, brokConfig)
	if err != nil {
		t.Fatalf("could not create broker: %s", err)
	}

	if _, err := broker.Consumer(NewConsumerConfig("does-not-exists", 421)); err != proto.ErrUnknownTopicOrPartition {
		t.Fatalf("expected %s error, got %s", proto.ErrUnknownTopicOrPartition, err)
	}

	consConfig := NewConsumerConfig("test", 1)
	// TODO(husio) this makes fetch from empty partition too slow for testing
	consConfig.FetchRetryLimit = 2
	consumer, err := broker.Consumer(consConfig)
	if err != nil {
		t.Fatalf("could not create consumer: %s", err)
	}

	_, err = consumer.Fetch()
	if err != ErrNoData {
		t.Fatalf("expected %s error, got %s", ErrNoData, err)
	}

	messages := []*proto.Message{
		&proto.Message{Key: []byte("1"), Value: []byte("first")},
		&proto.Message{Key: []byte("2"), Value: []byte("second")},
	}
	if _, err := srv.Logs.AddMessages("test", 1, messages); err != nil {
		t.Fatalf("could not add messages: %s", err)
	}

	msg1, err := consumer.Fetch()
	if err != nil {
		t.Fatalf("expected no errors, got %s", err)
	}
	if string(msg1.Value) != "first" || string(msg1.Key) != "1" || msg1.Offset != 1 {
		t.Fatalf("expected different message than %+q", msg1)
	}

	if err := broker.Close(); err != nil {
		t.Fatalf("could not close broker: %s", err)
	}
}
