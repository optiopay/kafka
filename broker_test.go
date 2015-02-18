package kafka

import (
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/optiopay/kafka/kafkatest"
	"github.com/optiopay/kafka/proto"
)

func TestingMetadataHandler(srv *kafkatest.Server) kafkatest.RequestHandler {
	return func(request kafkatest.Serializable) kafkatest.Serializable {
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
							ID:       0,
							Leader:   1,
							Replicas: []int32{1},
							Isrs:     []int32{1},
						},
						proto.MetadataRespPartition{
							ID:       1,
							Leader:   1,
							Replicas: []int32{1},
							Isrs:     []int32{1},
						},
					},
				},
			},
		}
	}
}

func TestDialWithInvalidAddress(t *testing.T) {
	srv := kafkatest.NewServer()
	srv.Start()
	defer srv.Close()

	addresses := []string{"localhost:4291190", "localhost:2141202", srv.Address()}
	conf := NewBrokerConf("tester")
	conf.DialTimeout = time.Millisecond * 200
	broker, err := Dial(addresses, conf)
	if err != nil {
		t.Fatalf("cannot create broker: %s", err)
	}
	broker.Close()
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

	conf := NewBrokerConf("tester")
	conf.DialTimeout = time.Millisecond * 200
	broker, err := Dial([]string{srv.Address()}, conf)
	if err != nil {
		t.Fatalf("cannot create broker: %s", err)
	}

	producer := broker.Producer(NewProducerConf())
	messages := []*proto.Message{
		&proto.Message{Value: []byte("first")},
		&proto.Message{Value: []byte("second")},
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
			crc := proto.ComputeCrc(msg)
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

	if messages[0].Offset != 5 || messages[1].Offset != 6 {
		t.Fatalf("message offset is incorrect: %#v", messages)
	}

	broker.Close()
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

	brokConf := NewBrokerConf("tester")
	brokConf.DialTimeout = time.Millisecond * 200
	broker, err := Dial([]string{srv.Address()}, brokConf)
	if err != nil {
		t.Fatalf("cannot create broker: %s", err)
	}

	if _, err := broker.Consumer(NewConsumerConf("does-not-exists", 413)); err != proto.ErrUnknownTopicOrPartition {
		t.Fatalf("expected %s error, got %s", proto.ErrUnknownTopicOrPartition, err)
	}
	if _, err := broker.Consumer(NewConsumerConf("test", 1)); err != proto.ErrUnknownTopicOrPartition {
		t.Fatalf("expected %s error, got %s", proto.ErrUnknownTopicOrPartition, err)
	}

	consConf := NewConsumerConf("test", 413)
	consConf.RetryWait = time.Millisecond
	consConf.StartOffset = 0
	consConf.RetryLimit = 4
	consumer, err := broker.Consumer(consConf)
	if err != nil {
		t.Fatalf("cannot create consumer: %s", err)
	}

	msg1, err := consumer.Fetch()
	if err != nil {
		t.Fatalf("expected no errors, got %s", err)
	}
	if string(msg1.Value) != "first" || string(msg1.Key) != "1" || msg1.Offset != 3 {
		t.Fatalf("expected different message than %#v", msg1)
	}

	msg2, err := consumer.Fetch()
	if err != nil {
		t.Fatalf("expected no errors, got %s", err)
	}
	if string(msg2.Value) != "second" || string(msg2.Key) != "2" || msg2.Offset != 4 {
		t.Fatalf("expected different message than %#v", msg2)
	}
	broker.Close()
}

func TestConsumerRetry(t *testing.T) {
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
	fetchCallCount := 0
	srv.Handle(kafkatest.FetchRequest, func(request kafkatest.Serializable) kafkatest.Serializable {
		req := request.(*proto.FetchReq)
		fetchCallCount++
		return &proto.FetchResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.FetchRespTopic{
				proto.FetchRespTopic{
					Name: "test",
					Partitions: []proto.FetchRespPartition{
						proto.FetchRespPartition{
							ID:        0,
							TipOffset: 0,
							Messages:  []*proto.Message{},
						},
					},
				},
			},
		}
	})

	brokConf := NewBrokerConf("test")
	brokConf.DialTimeout = time.Millisecond * 200
	broker, err := Dial([]string{srv.Address()}, brokConf)
	if err != nil {
		t.Fatalf("cannot create broker: %s", err)
	}

	consConf := NewConsumerConf("test", 0)
	consConf.RetryLimit = 5
	consConf.StartOffset = 0
	consConf.RetryWait = time.Millisecond
	consumer, err := broker.Consumer(consConf)
	if err != nil {
		t.Fatalf("cannot create consumer: %s", err)
	}

	if _, err := consumer.Fetch(); err != ErrNoData {
		t.Fatalf("expected %s error, got %s", ErrNoData, err)
	}
	if fetchCallCount != 6 {
		t.Fatalf("expected 6 fetch calls, got %d", fetchCallCount)
	}
	broker.Close()
}

func TestConsumeInvalidOffset(t *testing.T) {
	srv := kafkatest.NewServer()
	srv.Start()
	defer srv.Close()

	srv.Handle(kafkatest.MetadataRequest, TestingMetadataHandler(srv))

	srv.Handle(kafkatest.FetchRequest, func(request kafkatest.Serializable) kafkatest.Serializable {
		req := request.(*proto.FetchReq)
		messages := []*proto.Message{
			// return message with offset lower than requested
			&proto.Message{Offset: 3, Key: []byte("1"), Value: []byte("first")},
			&proto.Message{Offset: 4, Key: []byte("2"), Value: []byte("second")},
			&proto.Message{Offset: 5, Key: []byte("3"), Value: []byte("three")},
		}
		return &proto.FetchResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.FetchRespTopic{
				proto.FetchRespTopic{
					Name: "test",
					Partitions: []proto.FetchRespPartition{
						proto.FetchRespPartition{
							ID:        0,
							TipOffset: 2,
							Messages:  messages,
						},
					},
				},
			},
		}
	})

	brokConf := NewBrokerConf("tester")
	brokConf.DialTimeout = time.Millisecond * 200
	broker, err := Dial([]string{srv.Address()}, brokConf)
	if err != nil {
		t.Fatalf("cannot create broker: %s", err)
	}

	consConf := NewConsumerConf("test", 0)
	consConf.StartOffset = 4
	consumer, err := broker.Consumer(consConf)
	if err != nil {
		t.Fatalf("cannot create consumer: %s", err)
	}

	msg, err := consumer.Fetch()
	if err != nil {
		t.Fatalf("expected no errors, got %s", err)
	}
	if string(msg.Value) != "second" || string(msg.Key) != "2" || msg.Offset != 4 {
		t.Fatalf("expected different message than %#v", msg)
	}
	broker.Close()
}

func TestPartitionOffset(t *testing.T) {
	srv := kafkatest.NewServer()
	srv.Start()
	defer srv.Close()

	srv.Handle(kafkatest.MetadataRequest, TestingMetadataHandler(srv))

	var handlerErr error
	srv.Handle(kafkatest.OffsetRequest, func(request kafkatest.Serializable) kafkatest.Serializable {
		req := request.(*proto.OffsetReq)
		if req.ReplicaID != -1 {
			handlerErr = fmt.Errorf("expected -1 replica id, got %d", req.ReplicaID)
		}
		if req.Topics[0].Partitions[0].TimeMs != -2 {
			handlerErr = fmt.Errorf("expected -2 timems, got %d", req.Topics[0].Partitions[0].TimeMs)
		}
		return &proto.OffsetResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.OffsetRespTopic{
				proto.OffsetRespTopic{
					Name: "test",
					Partitions: []proto.OffsetRespPartition{
						proto.OffsetRespPartition{
							ID:      1,
							Offsets: []int64{123, 0},
						},
					},
				},
			},
		}
	})

	brokConf := NewBrokerConf("tester")
	brokConf.DialTimeout = time.Millisecond * 200
	broker, err := Dial([]string{srv.Address()}, brokConf)
	if err != nil {
		t.Fatalf("cannot create broker: %s", err)
	}

	offset, err := broker.offset("test", 1, -2)
	if handlerErr != nil {
		t.Fatalf("handler error: %s", handlerErr)
	}
	if err != nil {
		t.Fatalf("cannot fetch offset: %s", err)
	}
	if offset != 123 {
		t.Fatalf("expected 123 offset, got %d", offset)
	}
}

func TestLeaderConnectionFailover(t *testing.T) {
	srv := kafkatest.NewServer()
	srv.Start()
	defer srv.Close()

	srv.Handle(kafkatest.MetadataRequest, TestingMetadataHandler(srv))

	addresses := []string{srv.Address()}

	conf := NewBrokerConf("tester")
	conf.DialTimeout = time.Millisecond * 20
	conf.LeaderRetryWait = time.Millisecond
	conf.LeaderRetryLimit = 3

	broker, err := Dial(addresses, conf)
	if err != nil {
		t.Fatalf("cannot create broker: %s", err)
	}

	if _, err := broker.leaderConnection("does-not-exist", 123456); err != proto.ErrUnknownTopicOrPartition {
		t.Fatalf("%s expected, got %s", proto.ErrUnknownTopicOrPartition, err)
	}

	// fake initial metadata confuration
	broker.metadata.nodes = map[int32]string{
		1: "localhost:214412",
	}
	if _, err := broker.leaderConnection("test", 0); err == nil {
		t.Fatal("expected network error")
	}

	// provide node address that will be available after short period
	broker.metadata.nodes = map[int32]string{
		1: "localhost:23456",
	}
	conf.LeaderRetryWait = time.Millisecond
	conf.LeaderRetryLimit = 1000
	stop := make(chan struct{})

	go func() {
		for {
			if _, err := broker.leaderConnection("test", 0); err == nil {
				close(stop)
				return
			}
		}
	}()

	// let the leader election loop spin for a bit
	time.Sleep(time.Millisecond * 2)

	c, err := net.Listen("tcp", "localhost:23456")
	if err != nil {
		t.Fatalf("cannot start server: %s", err)
	}
	c.Accept()
	<-stop
	broker.Close()
}

func TestProducerFailoverRequestTimeout(t *testing.T) {
	srv := kafkatest.NewServer()
	srv.Start()
	defer srv.Close()

	srv.Handle(kafkatest.MetadataRequest, TestingMetadataHandler(srv))

	requestsCount := 0
	srv.Handle(kafkatest.ProduceRequest, func(request kafkatest.Serializable) kafkatest.Serializable {
		req := request.(*proto.ProduceReq)
		requestsCount++
		return &proto.ProduceResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.ProduceRespTopic{
				proto.ProduceRespTopic{
					Name: "test",
					Partitions: []proto.ProduceRespPartition{
						proto.ProduceRespPartition{
							ID:  0,
							Err: proto.ErrRequestTimeout,
						},
					},
				},
			},
		}
	})

	broker, err := Dial([]string{srv.Address()}, NewBrokerConf("test"))
	if err != nil {
		t.Fatalf("cannot create broker: %s", err)
	}

	prodConf := NewProducerConf()
	prodConf.RetryLimit = 4
	prodConf.RetryWait = time.Millisecond
	producer := broker.Producer(prodConf)

	_, err = producer.Produce("test", 0, &proto.Message{Value: []byte("first")}, &proto.Message{Value: []byte("second")})
	if err != proto.ErrRequestTimeout {
		t.Fatalf("expected %s, got %s", proto.ErrRequestTimeout, err)
	}
	if requestsCount != prodConf.RetryLimit {
		t.Fatalf("expected %d requests, got %d", prodConf.RetryLimit, requestsCount)
	}
}

func TestProducerFailoverLeaderNotAvailable(t *testing.T) {
	srv := kafkatest.NewServer()
	srv.Start()
	defer srv.Close()

	srv.Handle(kafkatest.MetadataRequest, TestingMetadataHandler(srv))

	requestsCount := 0
	srv.Handle(kafkatest.ProduceRequest, func(request kafkatest.Serializable) kafkatest.Serializable {
		req := request.(*proto.ProduceReq)
		requestsCount++

		if requestsCount > 4 {
			return &proto.ProduceResp{
				CorrelationID: req.CorrelationID,
				Topics: []proto.ProduceRespTopic{
					proto.ProduceRespTopic{
						Name: "test",
						Partitions: []proto.ProduceRespPartition{
							proto.ProduceRespPartition{
								ID:     0,
								Offset: 11,
							},
						},
					},
				},
			}
		}

		return &proto.ProduceResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.ProduceRespTopic{
				proto.ProduceRespTopic{
					Name: "test",
					Partitions: []proto.ProduceRespPartition{
						proto.ProduceRespPartition{
							ID:  0,
							Err: proto.ErrLeaderNotAvailable,
						},
					},
				},
			},
		}
	})

	broker, err := Dial([]string{srv.Address()}, NewBrokerConf("test"))
	if err != nil {
		t.Fatalf("cannot create broker: %s", err)
	}

	prodConf := NewProducerConf()
	prodConf.RetryLimit = 5
	prodConf.RetryWait = time.Millisecond
	producer := broker.Producer(prodConf)

	_, err = producer.Produce("test", 0, &proto.Message{Value: []byte("first")}, &proto.Message{Value: []byte("second")})
	if err != nil {
		t.Fatalf("expected no error, got %s", err)
	}
	if requestsCount != 5 {
		t.Fatalf("expected 5 requests, got %d", requestsCount)
	}
}

func TestConsumerFailover(t *testing.T) {
	srv := kafkatest.NewServer()
	srv.Start()
	defer srv.Close()

	messages := []*proto.Message{
		&proto.Message{Value: []byte("first")},
		&proto.Message{Value: []byte("second")},
	}

	srv.Handle(kafkatest.MetadataRequest, TestingMetadataHandler(srv))

	respCount := 0
	srv.Handle(kafkatest.FetchRequest, func(request kafkatest.Serializable) kafkatest.Serializable {
		respCount++
		req := request.(*proto.FetchReq)

		if respCount == 4 {
			return &proto.FetchResp{
				CorrelationID: req.CorrelationID,
				Topics: []proto.FetchRespTopic{
					proto.FetchRespTopic{
						Name: "test",
						Partitions: []proto.FetchRespPartition{
							proto.FetchRespPartition{
								ID:  1,
								Err: proto.ErrNotLeaderForPartition,
							},
						},
					},
				},
			}
		}
		resp := &proto.FetchResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.FetchRespTopic{
				proto.FetchRespTopic{
					Name: "test",
					Partitions: []proto.FetchRespPartition{
						proto.FetchRespPartition{
							ID:       1,
							Messages: messages,
						},
					},
				},
			},
		}
		messages = []*proto.Message{}
		return resp
	})

	broker, err := Dial([]string{srv.Address()}, NewBrokerConf("test"))
	if err != nil {
		t.Fatalf("cannot create broker: %s", err)
	}

	conf := NewConsumerConf("test", 1)
	conf.RetryWait = time.Nanosecond
	conf.RetryLimit = 4
	conf.RetryErrWait = time.Nanosecond
	conf.StartOffset = 0

	consumer, err := broker.Consumer(conf)
	if err != nil {
		t.Fatalf("cannot create consumer: %s", err)
	}

	for {
		msg, err := consumer.Fetch()
		if err != nil {
			t.Fatalf("failed to consume: %s", err)
		}
		if string(msg.Value) != "first" {
			t.Fatalf("expected first message got %#q", msg)
		}

		msg, err = consumer.Fetch()
		if err != nil {
			t.Fatalf("failed to consume: %s", err)
		}
		if string(msg.Value) != "second" {
			t.Fatalf("expected second message got %#q", msg)
		}

		if msg, err := consumer.Fetch(); err != ErrNoData {
			t.Fatalf("expected no data, got %#v (%#q)", err, msg)
		}

		return
	}
}

// this is not the best benchmark, because kafkatest.Server implementation is
// not made for performance, but it should be good enough to help tuning code.
func BenchmarkConsumer(b *testing.B) {
	const messagesPerResp = 30

	srv := kafkatest.NewServer()
	srv.Start()
	defer srv.Close()

	srv.Handle(kafkatest.MetadataRequest, TestingMetadataHandler(srv))

	var msgOffset int64
	srv.Handle(kafkatest.FetchRequest, func(request kafkatest.Serializable) kafkatest.Serializable {
		req := request.(*proto.FetchReq)
		messages := make([]*proto.Message, messagesPerResp)

		for i := range messages {
			msgOffset++
			msg := &proto.Message{
				Offset: msgOffset,
				Value:  []byte(`Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec a diam lectus. Sed sit amet ipsum mauris. Maecenas congue ligula ac quam viverra nec consectetur ante hendrerit. Donec et mollis dolor. Praesent et diam eget libero egestas mattis sit amet vitae augue. Nam tincidunt congue enim, ut porta lorem lacinia consectetur.`),
			}
			messages[i] = msg
		}
		return &proto.FetchResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.FetchRespTopic{
				proto.FetchRespTopic{
					Name: "test",
					Partitions: []proto.FetchRespPartition{
						proto.FetchRespPartition{
							ID:        0,
							TipOffset: msgOffset - int64(len(messages)),
							Messages:  messages,
						},
					},
				},
			},
		}
	})

	broker, err := Dial([]string{srv.Address()}, NewBrokerConf("test"))
	if err != nil {
		b.Fatalf("cannot create broker: %s", err)
	}

	conf := NewConsumerConf("test", 0)
	conf.StartOffset = 0

	consumer, err := broker.Consumer(conf)
	if err != nil {
		b.Fatalf("cannot create consumer: %s", err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := consumer.Fetch()
		if err != nil {
			b.Fatalf("cannot fetch message: %s", err)
		}
	}
}

func BenchmarkProducer(b *testing.B) {
	const messagesPerReq = 30

	srv := kafkatest.NewServer()
	srv.Start()
	defer srv.Close()

	srv.Handle(kafkatest.MetadataRequest, TestingMetadataHandler(srv))

	var msgOffset int64
	srv.Handle(kafkatest.ProduceRequest, func(request kafkatest.Serializable) kafkatest.Serializable {
		req := request.(*proto.ProduceReq)
		msgOffset += messagesPerReq
		return &proto.ProduceResp{
			CorrelationID: req.CorrelationID,
			Topics: []proto.ProduceRespTopic{
				proto.ProduceRespTopic{
					Name: "test",
					Partitions: []proto.ProduceRespPartition{
						proto.ProduceRespPartition{
							ID:     0,
							Offset: msgOffset,
						},
					},
				},
			},
		}
	})

	conf := NewBrokerConf("tester")
	conf.DialTimeout = time.Millisecond * 200
	broker, err := Dial([]string{srv.Address()}, conf)
	if err != nil {
		b.Fatalf("cannot create broker: %s", err)
	}

	messages := make([]*proto.Message, messagesPerReq)
	for i := range messages {
		msg := &proto.Message{
			Value: []byte(`Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec a diam lectus. Sed sit amet ipsum mauris. Maecenas congue ligula ac quam viverra nec consectetur ante hendrerit. Donec et mollis dolor. Praesent et diam eget libero egestas mattis sit amet vitae augue. Nam tincidunt congue enim, ut porta lorem lacinia consectetur.`),
		}
		messages[i] = msg
	}

	producer := broker.Producer(NewProducerConf())

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if _, err := producer.Produce("test", 0, messages...); err != nil {
			b.Fatalf("cannot produce message: %s", err)
		}

	}
}
