package integration

import (
	"strings"
	"testing"
	"time"

	"github.com/optiopay/kafka"
	"github.com/optiopay/kafka/proto"
)

func TestProducerBrokenConnection(t *testing.T) {
	IntegrationTest(t)

	topics := []string{"Topic1", "Topic2", "Topic3"}

	cluster := NewKafkaCluster("kafka-docker/", 4)
	if err := cluster.Start(); err != nil {
		t.Fatalf("cannot start kafka cluster: %s", err)
	}
	defer func() {
		_ = cluster.Stop()
	}()

	bconf := kafka.NewBrokerConf("producer-broken-connection")
	bconf.Log = &testLogger{t}
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
			if err := container.Stop(); err != nil {
				t.Fatalf("cannot stop %q kafka container: %s", container.ID, err)
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
		time.Sleep(1200 * time.Millisecond)
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
			t.Fatalf("cannot produce to %q: %s", name, err)
		}
	}

	for err := range errc {
		t.Fatalf("cannot start container: %s", err)
	}

	// make sure data was persisted
	for _, name := range topics {
		consumer, err := broker.Consumer(kafka.NewConsumerConf(name, 0))
		if err != nil {
			t.Fatalf("cannot create consumer for %q: %s", name, err)
		}
		for i := 0; i < 2; i++ {
			if _, err := consumer.Consume(); err != nil {
				t.Fatalf("cannot consume %d message from %q: %s", i, name, err)
			}
		}
	}
}

type testLogger struct {
	*testing.T
}

func (tlog *testLogger) Print(args ...interface{}) {
	tlog.Log(args...)
}

func (tlog *testLogger) Printf(format string, args ...interface{}) {
	tlog.Logf(format, args...)
}
