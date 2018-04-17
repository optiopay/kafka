package integration

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/optiopay/kafka"
	"github.com/optiopay/kafka/proto"
)

func TestProduceAndConsume(t *testing.T) {
	IntegrationTest(t)

	topics := []string{"Topic3", "Topic4"}

	cluster := NewKafkaCluster("kafka-docker/", 4)
	if err := cluster.Start(); err != nil {
		t.Fatalf("cannot start kafka cluster: %s", err)
	}
	defer func() {
		_ = cluster.Stop()
	}()

	if err := cluster.WaitUntilReady(); err != nil {
		t.Fatal(err)
	}

	bconf := kafka.NewBrokerConf("producer-broken-connection")
	bconf.Logger = &testLogger{t}
	addrs, err := cluster.KafkaAddrs()
	if err != nil {
		t.Fatalf("cannot get kafka address: %s", err)
	}
	broker, err := kafka.Dial(addrs, bconf)
	if err != nil {
		t.Fatalf("cannot connect to cluster (%q): %s", addrs, err)
	}
	defer broker.Close()

	// produce big message to enforce TCP buffer flush
	m := proto.Message{
		//Value: []byte(strings.Repeat("producer broken connection message ", 1000)),
		Value: []byte("hello world"),
	}
	pconf := kafka.NewProducerConf()
	producer := broker.Producer(pconf)

	// send message to all topics to make sure it's working
	for _, name := range topics {
		if _, err := producer.Produce(name, 0, &m); err != nil {
			t.Fatalf("cannot produce to %q: %s", name, err)
		}
	}

	time.Sleep(5 * time.Second)

	// make sure data was persisted
	for _, name := range topics {
		consumer, err := broker.Consumer(kafka.NewConsumerConf(name, 0))
		if err != nil {
			t.Errorf("cannot create consumer for %q: %s", name, err)
			continue
		}
		m1, err := consumer.Consume()
		if err != nil {
			t.Errorf("cannot consume %d message from %q: %s", 0, name, err)
		} else {
			if !reflect.DeepEqual(m.Value, m1.Value) {
				t.Errorf("Got different message. Wait:\n%#+v   got:\n%#+v", m.Value, m1.Value)
			}

		}
	}

	// check if offsets are correct
	for _, name := range topics {
		offe, err := broker.OffsetEarliest(name, 0)
		if err != nil {
			t.Fatal(err)
		}
		if offe != 0 {
			t.Fatalf("Should get OffsetEarliest == 0 Got %#v instead ", offe)
		}
		offl, err := broker.OffsetLatest(name, 0)
		if err != nil {
			t.Fatal(err)
		}
		if offl != 1 {
			t.Fatalf("Should get OffsetLatest == 1. Got %#v instead ", offl)
		}

	}

}

func TestCompression(t *testing.T) {
	IntegrationTest(t)

	topics := []string{"Topic3", "Topic4"}

	cluster := NewKafkaCluster("kafka-docker/", 4)
	if err := cluster.Start(); err != nil {
		t.Fatalf("cannot start kafka cluster: %s", err)
	}
	defer func() {
		_ = cluster.Stop()
	}()

	if err := cluster.WaitUntilReady(); err != nil {
		t.Fatal(err)
	}

	bconf := kafka.NewBrokerConf("producer-broken-connection")
	bconf.Logger = &testLogger{t}
	addrs, err := cluster.KafkaAddrs()
	if err != nil {
		t.Fatalf("cannot get kafka address: %s", err)
	}
	broker, err := kafka.Dial(addrs, bconf)
	if err != nil {
		t.Fatalf("cannot connect to cluster (%q): %s", addrs, err)
	}
	defer broker.Close()

	// produce big message to enforce TCP buffer flush
	m := proto.Message{
		//Value: []byte(strings.Repeat("producer broken connection message ", 1000)),
		Value: []byte("hello world"),
	}

	// Use GZIP compression for Topic3
	pconf := kafka.NewProducerConf()
	pconf.Compression = proto.CompressionGzip
	producer := broker.Producer(pconf)

	// send message to all topics to make sure it's working
	//for i := 0; i < 2; i++ {
	if _, err := producer.Produce(topics[0], 0, &m); err != nil {
		t.Fatalf("cannot produce to %q: %s", topics[0], err)
	}

	// Use Snappy compression for Topic4
	pconf = kafka.NewProducerConf()
	pconf.Compression = proto.CompressionSnappy
	producer = broker.Producer(pconf)

	if _, err := producer.Produce(topics[1], 0, &m); err != nil {
		t.Fatalf("cannot produce to %q: %s", topics[1], err)
	}

	time.Sleep(5 * time.Second)

	// make sure data was persisted
	for _, name := range topics {
		consumer, err := broker.Consumer(kafka.NewConsumerConf(name, 0))
		if err != nil {
			t.Errorf("cannot create consumer for %q: %s", name, err)
			continue
		}
		//for i := 0; i < 2; i++ {
		m1, err := consumer.Consume()
		if err != nil {
			t.Errorf("cannot consume %d message from %q: %s", 0, name, err)
		} else {

			if !reflect.DeepEqual(m.Value, m1.Value) {
				t.Errorf("Got different message. Wait:\n%#+v   got:\n%#+v", m.Value, m1.Value)

			}

		}
	}

}

func TestProducerBrokenConnection(t *testing.T) {
	IntegrationTest(t)

	topics := []string{"Topic3", "Topic4"}

	cluster := NewKafkaCluster("kafka-docker/", 4)
	if err := cluster.Start(); err != nil {
		t.Fatalf("cannot start kafka cluster: %s", err)
	}
	defer func() {
		_ = cluster.Stop()
	}()

	if err := cluster.WaitUntilReady(); err != nil {
		t.Fatal(err)
	}

	bconf := kafka.NewBrokerConf("producer-broken-connection")
	bconf.Logger = &testLogger{t}
	addrs, err := cluster.KafkaAddrs()
	if err != nil {
		t.Fatalf("cannot get kafka address: %s", err)
	}
	broker, err := kafka.Dial(addrs, bconf)
	if err != nil {
		t.Fatalf("cannot connect to cluster (%q): %s", addrs, err)
	}
	defer broker.Close()

	// produce big message to enforce TCP buffer flush
	m := proto.Message{
		Value: []byte(strings.Repeat("producer broken connection message ", 1000)),
	}
	pconf := kafka.NewProducerConf()
	producer := broker.Producer(pconf)

	// send message to all topics to make sure it's working
	for _, name := range topics {
		if _, err := producer.Produce(name, 0, &m); err != nil {
			t.Fatalf("cannot produce to %q: %s", name, err)
		}
	}

	// close two kafka clusters and publish to all 3 topics - 2 of them should
	// retry sending, because lack of leader makes the request fail
	//
	// request should not succeed until nodes are back - bring them back after
	// small delay and make sure producing was successful
	containers, err := cluster.Containers()
	if err != nil {
		t.Fatalf("cannot get containers: %s", err)
	}
	var stopped []*Container
	for _, container := range containers {
		if container.RunningKafka() {
			if err := container.Kill(); err != nil {
				t.Fatalf("cannot kill %q kafka container: %s", container.ID, err)
			}
			stopped = append(stopped, container)
		}
		if len(stopped) == 2 {
			break
		}
	}

	// bring stopped containers back
	errc := make(chan error)
	go func() {
		time.Sleep(500 * time.Millisecond)
		for _, container := range stopped {
			if err := container.Start(); err != nil {
				errc <- err
			}
		}
		close(errc)
	}()

	// send message to all topics to make sure it's working
	for _, name := range topics {
		if _, err := producer.Produce(name, 0, &m); err != nil {
			t.Errorf("cannot produce to %q: %s", name, err)
		}
	}

	for err := range errc {
		t.Errorf("cannot start container: %s", err)
	}

	// make sure data was persisted
	for _, name := range topics {
		consumer, err := broker.Consumer(kafka.NewConsumerConf(name, 0))
		if err != nil {
			t.Errorf("cannot create consumer for %q: %s", name, err)
			continue
		}
		for i := 0; i < 2; i++ {
			if _, err := consumer.Consume(); err != nil {
				t.Errorf("cannot consume %d message from %q: %s", i, name, err)
			}
		}
	}
}

func TestConsumerBrokenConnection(t *testing.T) {
	IntegrationTest(t)
	const msgPerTopic = 10

	topics := []string{"Topic3", "Topic4"}

	cluster := NewKafkaCluster("kafka-docker/", 5)
	if err := cluster.Start(); err != nil {
		t.Fatalf("cannot start kafka cluster: %s", err)
	}
	defer func() {
		_ = cluster.Stop()
	}()
	if err := cluster.WaitUntilReady(); err != nil {
		t.Fatal(err)
	}

	bconf := kafka.NewBrokerConf("producer-broken-connection")
	bconf.Logger = &testLogger{t}
	addrs, err := cluster.KafkaAddrs()
	if err != nil {
		t.Fatalf("cannot get kafka address: %s", err)
	}
	broker, err := kafka.Dial(addrs, bconf)
	if err != nil {
		t.Fatalf("cannot connect to cluster (%q): %s", addrs, err)
	}
	defer broker.Close()

	// produce big message to enforce TCP buffer flush
	m := proto.Message{
		Value: []byte(strings.Repeat("consumer broken connection message ", 1000)),
	}
	pconf := kafka.NewProducerConf()
	producer := broker.Producer(pconf)

	// send message to all topics
	for _, name := range topics {
		for i := 0; i < msgPerTopic; i++ {
			if _, err := producer.Produce(name, 0, &m); err != nil {
				t.Fatalf("cannot produce to %q: %s", name, err)
			}
		}
	}

	// close two kafka clusters and publish to all 3 topics - 2 of them should
	// retry sending, because lack of leader makes the request fail
	//
	// request should not succeed until nodes are back - bring them back after
	// small delay and make sure producing was successful
	containers, err := cluster.Containers()
	if err != nil {
		t.Fatalf("cannot get containers: %s", err)
	}
	var stopped []*Container
	for _, container := range containers {
		if container.RunningKafka() {
			if err := container.Kill(); err != nil {
				t.Fatalf("cannot kill %q kafka container: %s", container.ID, err)
			}
			stopped = append(stopped, container)
		}
		if len(stopped) == 2 {
			break
		}
	}

	// bring stopped containers back
	errc := make(chan error)
	go func() {
		time.Sleep(500 * time.Millisecond)
		for _, container := range stopped {
			if err := container.Start(); err != nil {
				errc <- err
			}
		}
		close(errc)
	}()

	time.Sleep(5 * time.Second)

	// make sure data was persisted
	for _, name := range topics {
		consumer, err := broker.Consumer(kafka.NewConsumerConf(name, 0))
		if err != nil {
			t.Errorf("cannot create consumer for %q: %s", name, err)
			continue
		}
		for i := 0; i < msgPerTopic; i++ {
			if _, err := consumer.Consume(); err != nil {
				t.Errorf("cannot consume %d message from %q: %s", i, name, err)
			}
		}
	}

	for err := range errc {
		t.Errorf("cannot start container: %s", err)
	}
}

func TestNewTopic(t *testing.T) {
	IntegrationTest(t)
	const msgPerTopic = 10

	topic := "NewTopic"

	cluster := NewKafkaCluster("kafka-docker/", 1)
	if err := cluster.Start(); err != nil {
		t.Fatalf("cannot start kafka cluster: %s", err)
	}
	defer func() {
		_ = cluster.Stop()
	}()

	// We cannot use here cluster.WaitUntilReady() because
	// it waits until topics will be created. But in this
	// tests we create cluster with 1 cluster node only,
	// which means that topic creation will fail
	// because they require 4 partitions
	// So we can only wait and hope that 10 seconds is enough

	time.Sleep(10 * time.Second)

	//time.Sleep(5 * time.Second)
	//	go func() {
	//		logCmd := cluster.cmd("docker-compose", "logs")
	//		if err := logCmd.Run(); err != nil {
	//			panic(err)
	//		}
	//	}()

	bconf := kafka.NewBrokerConf("producer-new-topic")
	bconf.Logger = &testLogger{t}
	addrs, err := cluster.KafkaAddrs()
	if err != nil {
		t.Fatalf("cannot get kafka address: %s", err)
	}
	broker, err := kafka.Dial(addrs, bconf)
	if err != nil {
		t.Fatalf("cannot connect to cluster (%q): %s", addrs, err)
	}
	defer broker.Close()

	topicInfo := proto.TopicInfo{
		Topic:             topic,
		NumPartitions:     1,
		ReplicationFactor: 1,
	}

	resp, err := broker.CreateTopic([]proto.TopicInfo{topicInfo}, 10*time.Second, false)

	if err != nil {
		t.Fatal(err)
	}

	for _, e := range resp.TopicErrors {
		if e.ErrorCode != 0 {
			t.Fatalf("Got error on topic creation %#+v", e)
		}
	}

	m := proto.Message{
		Value: []byte("Test message"),
	}
	pconf := kafka.NewProducerConf()
	producer := broker.Producer(pconf)

	if _, err := producer.Produce(topic, 0, &m); err != nil {
		t.Fatalf("cannot produce to %q: %s", topic, err)
	}

	consumer, err := broker.Consumer(kafka.NewConsumerConf(topic, 0))
	if err != nil {
		t.Fatalf("cannot create consumer for %q: %s", topic, err)
	}
	if _, err := consumer.Consume(); err != nil {
		t.Errorf("cannot consume message from %q: %s", topic, err)
	}
}

type testLogger struct {
	*testing.T
}

func (tlog *testLogger) Debug(msg string, keyvals ...interface{}) {
	args := append([]interface{}{msg}, keyvals...)
	tlog.Log(args...)
}

func (tlog *testLogger) Info(msg string, keyvals ...interface{}) {
	args := append([]interface{}{msg}, keyvals...)
	tlog.Log(args...)
}

func (tlog *testLogger) Warn(msg string, keyvals ...interface{}) {
	args := append([]interface{}{msg}, keyvals...)
	tlog.Log(args...)
}

func (tlog *testLogger) Error(msg string, keyvals ...interface{}) {
	args := append([]interface{}{msg}, keyvals...)
	tlog.Log(args...)
}
