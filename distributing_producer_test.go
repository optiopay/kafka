package kafka

import (
	"sync"
	"testing"

	"github.com/optiopay/kafka/proto"
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
		[][]byte{
			[]byte("a 1"),
			[]byte("a 2"),
		},
		[][]byte{
			[]byte("b 1"),
		},
		[][]byte{
			[]byte("c 1"),
			[]byte("c 2"),
			[]byte("c 3"),
		},
		[][]byte{
			[]byte("d 1"),
		},
	}

	for _, values := range data {
		msgs := make([]*proto.Message, 0)
		for _, value := range values {
			msgs = append(msgs, &proto.Message{Value: value})
		}
		p.Distribute("test-topic", msgs...)
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
