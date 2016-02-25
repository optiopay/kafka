package kafka

import (
	"errors"
	"fmt"
	"github.com/optiopay/kafka/proto"
	"sync"
	"testing"
	"time"
)

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

func TestRoundRobinProducer(t *testing.T) {
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
			t.Errorf("cannot distribute %d message: %s", i, err)
		}
	}

	// a, [0, 1]
	if rec.msgs[0].Partition != 0 || rec.msgs[1].Partition != 0 {
		t.Fatalf("expected partition 0, got %d and %d", rec.msgs[0].Partition, rec.msgs[1].Partition)
	}

	// b, [2]
	if rec.msgs[2].Partition != 1 {
		t.Fatalf("expected partition 1, got %d", rec.msgs[2].Partition)
	}

	// c, [3, 4, 5]
	if rec.msgs[3].Partition != 2 || rec.msgs[4].Partition != 2 {
		t.Fatalf("expected partition 2, got %d and %d", rec.msgs[3].Partition, rec.msgs[3].Partition)
	}

	// d, [6]
	if rec.msgs[6].Partition != 0 {
		t.Fatalf("expected partition 0, got %d", rec.msgs[6].Partition)
	}
}

func TestHashProducer(t *testing.T) {
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
			t.Errorf("cannot distribute %d message: %s", i, err)
		}
	}

	if len(rec.msgs) != len(keys) {
		t.Fatalf("expected %d messages, got %d", len(keys), len(rec.msgs))
	}

	for i, key := range keys {
		want, err := messageHashPartition(key, parts)
		if err != nil {
			t.Errorf("cannot compute hash: %s", err)
			continue
		}
		if got := rec.msgs[i].Partition; want != got {
			t.Errorf("expected partition %d, got %d", want, got)
		} else if got > parts-1 {
			t.Errorf("number of partitions is %d, but message written to %d", parts, got)
		}
	}
}

func TestBackoff(t *testing.T) {
	err := errors.New("An error")
	backoff := newBackOffType(10*time.Second, 1.5)
	if out := backoff.WaitTime(nil); out != 0 {
		t.Fatalf("Expected 0 wait time, got %v", out)
	}
	if out := backoff.WaitTime(err); out != 10*time.Second {
		t.Fatalf("Expeccted %v, got %v", 10*time.Second, out)
	}
	if out := backoff.WaitTime(err); out != 15*time.Second {
		t.Fatalf("Expeccted %v, got %v", 15*time.Second, out)
	}
	if out := backoff.WaitTime(err); out != 22*time.Second+500*time.Millisecond {
		t.Fatalf("Expeccted %v, got %v", 22*time.Second+500*time.Millisecond, out)
	}
	if out := backoff.WaitTime(nil); out != 0 {
		t.Fatalf("Expected 0 wait time, got %v", out)
	}
	if out := backoff.WaitTime(err); out != 10*time.Second {
		t.Fatalf("Expeccted %v, got %v", 10*time.Second, out)
	}
}
