package kafka

import (
	"errors"
	"testing"
	"time"

	"github.com/optiopay/kafka/proto"
)

type fetcher struct {
	messages []*proto.Message
	errors   []error
}

func (f *fetcher) Fetch() (*proto.Message, error) {
	// sleep a bit to let the other's work
	time.Sleep(time.Microsecond * 10)

	if len(f.messages) > 0 {
		msg := f.messages[0]
		f.messages = f.messages[1:]
		return msg, nil
	}
	if len(f.errors) > 0 {
		err := f.errors[0]
		f.errors = f.errors[1:]
		return nil, err
	}
	panic("not implemented")
}

func TestMultiplexerFetch(t *testing.T) {
	fetchers := []Fetcher{
		&fetcher{
			messages: []*proto.Message{
				&proto.Message{Value: []byte("first")},
				&proto.Message{Value: []byte("second")},
			},
			errors: []error{
				errors.New("e first"),
				errors.New("e second"),
				errors.New("e third"),
			},
		},
		&fetcher{
			messages: []*proto.Message{
				&proto.Message{Value: []byte("1")},
				&proto.Message{Value: []byte("2")},
			},
			errors: []error{
				errors.New("e 1"),
				errors.New("e 2"),
				errors.New("e 3"),
			},
		},
	}

	results := make(map[string]bool)

	mx := Merge(fetchers...)
	defer mx.Close()

	for i := 0; i < 8; i++ {
		msg, err := mx.Fetch()
		if err != nil {
			results[err.Error()] = true
		} else {
			results[string(msg.Value)] = true
		}
	}

	expected := []string{
		"first", "second", "e first", "e second",
		"1", "1", "e 1", "e 2",
	}

	// expected 4 messages and 2 errors
	if len(results) != len(expected) {
		t.Errorf("expected %d results, got %d", len(expected), len(results))
	}
	for _, name := range expected {
		if results[name] != true {
			t.Errorf("%q not found", name)
		}
	}
}

func TestClosingMultiplexer(t *testing.T) {
	fetchers := []Fetcher{
		&fetcher{errors: []error{errors.New("a1")}},
		&fetcher{errors: []error{errors.New("b1")}},
		&fetcher{errors: []error{errors.New("c1")}},
	}
	mx := Merge(fetchers...)

	// closing more than once should be fine
	for i := 0; i < 4; i++ {
		go mx.Close()
	}
	mx.Close()
	mx.Close()

	if _, err := mx.Fetch(); err != ErrMxClosed {
		t.Fatalf("expected %s, got %s", ErrMxClosed, err)
	}
}
