package kafka

import (
	"fmt"
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

	srv.Handle(kafkatest.MetadataRequest, func(request kafkatest.Serializable) kafkatest.Serializable {
		req, ok := request.(*proto.MetadataReq)
		if !ok {
			panic(fmt.Sprintf("expected metadata request, got %T", request))
		}
		host, port := srv.HostPort()
		return &proto.MetadataResp{
			CorrelationID: req.CorrelationID,
			Brokers: []proto.MetadataRespBroker{
				proto.MetadataRespBroker{NodeID: 1, Host: host, Port: int32(port)},
			},
			Topics: []proto.MetadataRespTopic{
				proto.MetadataRespTopic{
					Name: "test",
					Partitions: []proto.MetadataRespPartition{
						proto.MetadataRespPartition{
							ID:       0,
							Leader:   1,
							Replicas: []int32{1},
							Isrs:     []int32{1},
						},
					},
				},
			},
		}
	})

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

	var handleErr error
	srv.Handle(kafkatest.ProduceRequest, func(request kafkatest.Serializable) kafkatest.Serializable {
		req := request.(*proto.ProduceReq)
		if req.Topics[0].Name != "test" {
			handleErr = fmt.Errorf("expected 'test' topic, got %s", req.Topics[0].Name)
			return nil
		}
		if req.Topics[0].Partitions[0].ID != 0 {
			handleErr = fmt.Errorf("expected 0 partition, got %s", req.Topics[0].Partitions[0].ID)
			return nil
		}
		messages := req.Topics[0].Partitions[0].Messages
		for _, msg := range messages {
			crc := kafkatest.ComputeCrc(msg)
			if msg.Crc != crc {
				handleErr = fmt.Errorf("expected '%s' crc, got %s", crc, msg.Crc)
				return nil
			}
		}
		return &proto.ProduceResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.ProduceRespTopic{
				proto.ProduceRespTopic{
					Name: "test",
					Partitions: []proto.ProduceRespPartition{
						proto.ProduceRespPartition{
							ID:     0,
							Offset: 5,
						},
					},
				},
			},
		}
	})

	offset, err := producer.Produce("test", 0, messages...)
	if handleErr != nil {
		t.Fatalf("handling error: %s", handleErr)
	}
	if err != nil {
		t.Fatalf("expected no error, got %s", err)
	}
	if offset != 5 {
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

	srv.Handle(kafkatest.MetadataRequest, func(request kafkatest.Serializable) kafkatest.Serializable {
		req := request.(*proto.MetadataReq)
		host, port := srv.HostPort()
		return &proto.MetadataResp{
			CorrelationID: req.CorrelationID,
			Brokers: []proto.MetadataRespBroker{
				proto.MetadataRespBroker{NodeID: 1, Host: host, Port: int32(port)},
			},
			Topics: []proto.MetadataRespTopic{
				proto.MetadataRespTopic{
					Name: "test",
					Partitions: []proto.MetadataRespPartition{
						proto.MetadataRespPartition{
							ID:       413,
							Leader:   1,
							Replicas: []int32{1},
							Isrs:     []int32{1},
						},
					},
				},
			},
		}
	})
	fetchCallCount := 0
	srv.Handle(kafkatest.FetchRequest, func(request kafkatest.Serializable) kafkatest.Serializable {
		req := request.(*proto.FetchReq)
		fetchCallCount++
		if fetchCallCount < 2 {
			return &proto.FetchResp{
				CorrelationID: req.CorrelationID,
				Topics: []proto.FetchRespTopic{
					proto.FetchRespTopic{
						Name: "test",
						Partitions: []proto.FetchRespPartition{
							proto.FetchRespPartition{
								ID:        413,
								TipOffset: 0,
								Messages:  []*proto.Message{},
							},
						},
					},
				},
			}
		}

		messages := []*proto.Message{
			&proto.Message{Offset: 3, Key: []byte("1"), Value: []byte("first")},
			&proto.Message{Offset: 4, Key: []byte("2"), Value: []byte("second")},
			&proto.Message{Offset: 5, Key: []byte("3"), Value: []byte("three")},
		}
		for _, m := range messages {
			m.Crc = kafkatest.ComputeCrc(m)
		}

		return &proto.FetchResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.FetchRespTopic{
				proto.FetchRespTopic{
					Name: "test",
					Partitions: []proto.FetchRespPartition{
						proto.FetchRespPartition{
							ID:        413,
							TipOffset: 2,
							Messages:  messages,
						},
					},
				},
			},
		}
	})

	brokConfig := NewBrokerConfig("tester")
	brokConfig.DialTimeout = time.Millisecond * 200
	broker, err := Dial([]string{srv.Address()}, brokConfig)
	if err != nil {
		t.Fatalf("could not create broker: %s", err)
	}

	if _, err := broker.Consumer(NewConsumerConfig("does-not-exists", 413)); err != proto.ErrUnknownTopicOrPartition {
		t.Fatalf("expected %s error, got %s", proto.ErrUnknownTopicOrPartition, err)
	}
	if _, err := broker.Consumer(NewConsumerConfig("test", 1)); err != proto.ErrUnknownTopicOrPartition {
		t.Fatalf("expected %s error, got %s", proto.ErrUnknownTopicOrPartition, err)
	}

	consConfig := NewConsumerConfig("test", 413)
	consConfig.FetchRetryLimit = 4
	consumer, err := broker.Consumer(consConfig)
	if err != nil {
		t.Fatalf("could not create consumer: %s", err)
	}

	msg1, err := consumer.Fetch()
	if err != nil {
		t.Fatalf("expected no errors, got %s", err)
	}
	if string(msg1.Value) != "first" || string(msg1.Key) != "1" || msg1.Offset != 3 {
		t.Fatalf("expected different message than %+q", msg1)
	}

	msg2, err := consumer.Fetch()
	if err != nil {
		t.Fatalf("expected no errors, got %s", err)
	}
	if string(msg2.Value) != "second" || string(msg2.Key) != "2" || msg2.Offset != 4 {
		t.Fatalf("expected different message than %+q", msg2)
	}

	if err := broker.Close(); err != nil {
		t.Fatalf("could not close broker: %s", err)
	}
}
