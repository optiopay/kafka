package kafka

import (
	"fmt"
	"sync"

	. "gopkg.in/check.v1"

	"github.com/optiopay/kafka/proto"
)

var _ = Suite(&DistProducerSuite{})

type DistProducerSuite struct {
	l *testLogger
}

func (s *DistProducerSuite) SetUpTest(c *C) {
	s.l = &testLogger{c: c}
}

type recordingProducer struct {
	sync.Mutex
	msgs []*proto.Message
}

func newRecordingProducer() *recordingProducer {
	return &recordingProducer{msgs: make([]*proto.Message, 0)}
}

func (p *recordingProducer) Produce(topic string, part int32, msgs ...*proto.Message) (int64, error) {
	p.Lock()
	defer p.Unlock()

	offset := len(p.msgs)
	p.msgs = append(p.msgs, msgs...)
	for i, msg := range msgs {
		msg.Offset = int64(offset + i)
		msg.Topic = topic
		msg.Partition = part
	}
	return int64(len(p.msgs)), nil
}

func (s *DistProducerSuite) TestRoundRobinProducer(c *C) {
	rec := newRecordingProducer()
	p := NewRoundRobinProducer(rec, 3)

	data := [][][]byte{
		{
			[]byte("a 1"),
			[]byte("a 2"),
		},
		{
			[]byte("b 1"),
		},
		{
			[]byte("c 1"),
			[]byte("c 2"),
			[]byte("c 3"),
		},
		{
			[]byte("d 1"),
		},
	}

	for i, values := range data {
		msgs := make([]*proto.Message, 0)
		for _, value := range values {
			msgs = append(msgs, &proto.Message{Value: value})
		}
		if _, err := p.Distribute("test-topic", msgs...); err != nil {
			c.Errorf("cannot distribute %d message: %s", i, err)
		}
	}

	// a, [0, 1]
	if rec.msgs[0].Partition != 0 || rec.msgs[1].Partition != 0 {
		c.Fatalf("expected partition 0, got %d and %d", rec.msgs[0].Partition, rec.msgs[1].Partition)
	}

	// b, [2]
	if rec.msgs[2].Partition != 1 {
		c.Fatalf("expected partition 1, got %d", rec.msgs[2].Partition)
	}

	// c, [3, 4, 5]
	if rec.msgs[3].Partition != 2 || rec.msgs[4].Partition != 2 {
		c.Fatalf("expected partition 2, got %d and %d", rec.msgs[3].Partition, rec.msgs[3].Partition)
	}

	// d, [6]
	if rec.msgs[6].Partition != 0 {
		c.Fatalf("expected partition 0, got %d", rec.msgs[6].Partition)
	}
}

func (s *DistProducerSuite) TestHashProducer(c *C) {
	const parts = 3
	rec := newRecordingProducer()
	p := NewHashProducer(rec, parts)

	var keys [][]byte
	for i := 0; i < 30; i++ {
		keys = append(keys, []byte(fmt.Sprintf("key-%d", i)))
	}
	for i, key := range keys {
		msg := &proto.Message{Key: key}
		if _, err := p.Distribute("test-topic", msg); err != nil {
			c.Errorf("cannot distribute %d message: %s", i, err)
		}
	}

	if len(rec.msgs) != len(keys) {
		c.Fatalf("expected %d messages, got %d", len(keys), len(rec.msgs))
	}

	for i, key := range keys {
		want, err := messageHashPartition(key, parts)
		if err != nil {
			c.Errorf("cannot compute hash: %s", err)
			continue
		}
		if got := rec.msgs[i].Partition; want != got {
			c.Errorf("expected partition %d, got %d", want, got)
		} else if got > parts-1 {
			c.Errorf("number of partitions is %d, but message written to %d", parts, got)
		}
	}
}
