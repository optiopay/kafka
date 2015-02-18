package kafka

import (
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/optiopay/kafka/proto"
)

type serializableMessage interface {
	Bytes() ([]byte, error)
}

func testServer(messages ...serializableMessage) (net.Listener, error) {
	ln, err := net.Listen("tcp", "")
	if err != nil {
		return nil, err
	}

	responses := make([][]byte, len(messages))
	for i, m := range messages {
		b, err := m.Bytes()
		if err != nil {
			ln.Close()
			return nil, err
		}
		responses[i] = b
	}

	go func() {
		for {
			cli, err := ln.Accept()
			if err != nil {
				return
			}

			go func(conn net.Conn) {
				for _, resp := range responses {
					cli.Write(resp)
				}
			}(cli)
		}
	}()
	return ln, nil
}

func TestConnectionMetadata(t *testing.T) {
	resp1 := &proto.MetadataResp{
		CorrelationID: 1,
		Brokers: []proto.MetadataRespBroker{
			proto.MetadataRespBroker{
				NodeID: 666,
				Host:   "example.com",
				Port:   999,
			},
		},
		Topics: []proto.MetadataRespTopic{
			proto.MetadataRespTopic{
				Name: "foo",
				Partitions: []proto.MetadataRespPartition{
					proto.MetadataRespPartition{
						ID:       7,
						Leader:   7,
						Replicas: []int32{7},
						Isrs:     []int32{7},
					},
				},
			},
		},
	}
	ln, err := testServer(resp1)
	if err != nil {
		t.Fatalf("test server error: %s", err)
	}
	conn, err := NewConnection(ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("could not conect to test server: %s", err)
	}
	resp, err := conn.Metadata(&proto.MetadataReq{
		ClientID: "tester",
		Topics:   []string{"first", "second"},
	})
	if err != nil {
		t.Fatalf("could not fetch response: %s", err)
	}
	if !reflect.DeepEqual(resp, resp1) {
		t.Fatalf("expected different response %#v", resp)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("could not close kafka connection: %s", err)
	}
	if err := ln.Close(); err != nil {
		t.Fatalf("could not close test server: %s", err)
	}
}

func TestConnectionProduce(t *testing.T) {
	resp1 := &proto.ProduceResp{
		CorrelationID: 1,
		Topics: []proto.ProduceRespTopic{
			proto.ProduceRespTopic{
				Name: "first",
				Partitions: []proto.ProduceRespPartition{
					proto.ProduceRespPartition{
						ID:     0,
						Err:    nil,
						Offset: 4,
					},
				},
			},
		},
	}
	resp2 := &proto.ProduceResp{
		CorrelationID: 2,
		Topics: []proto.ProduceRespTopic{
			proto.ProduceRespTopic{
				Name: "first",
				Partitions: []proto.ProduceRespPartition{
					proto.ProduceRespPartition{
						ID:     0,
						Err:    proto.ErrLeaderNotAvailable,
						Offset: -1,
					},
				},
			},
		},
	}

	// sending in random order should work as well
	ln, err := testServer(resp2, resp1)
	if err != nil {
		t.Fatalf("test server error: %s", err)
	}
	conn, err := NewConnection(ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("could not conect to test server: %s", err)
	}
	resp, err := conn.Produce(&proto.ProduceReq{
		ClientID:     "tester",
		RequiredAcks: proto.RequiredAcksAll,
		Timeout:      time.Second,
		Topics: []proto.ProduceReqTopic{
			proto.ProduceReqTopic{
				Name: "first",
				Partitions: []proto.ProduceReqPartition{
					proto.ProduceReqPartition{
						ID: 0,
						Messages: []*proto.Message{
							&proto.Message{Key: []byte("key 1"), Value: []byte("value 1")},
							&proto.Message{Key: []byte("key 2"), Value: []byte("value 2")},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("could not fetch response: %s", err)
	}
	if !reflect.DeepEqual(resp, resp1) {
		t.Fatalf("expected different response %#v", resp)
	}
	resp, err = conn.Produce(&proto.ProduceReq{
		ClientID:     "tester",
		RequiredAcks: proto.RequiredAcksAll,
		Timeout:      time.Second,
		Topics: []proto.ProduceReqTopic{
			proto.ProduceReqTopic{
				Name: "first",
				Partitions: []proto.ProduceReqPartition{
					proto.ProduceReqPartition{
						ID: 0,
						Messages: []*proto.Message{
							&proto.Message{Key: []byte("key"), Value: []byte("value")},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("could not fetch response: %s", err)
	}
	if !reflect.DeepEqual(resp, resp2) {
		t.Fatalf("expected different response %#v", resp)
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("could not close kafka connection: %s", err)
	}
	if err := ln.Close(); err != nil {
		t.Fatalf("could not close test server: %s", err)
	}
}

func TestConnectionFetch(t *testing.T) {
	messages := []*proto.Message{
		&proto.Message{Offset: 4, Key: []byte("f"), Value: []byte("first")},
		&proto.Message{Offset: 5, Key: []byte("s"), Value: []byte("second message")},
	}
	for _, m := range messages {
		m.Crc = proto.ComputeCrc(m)
	}

	resp1 := &proto.FetchResp{
		CorrelationID: 1,
		Topics: []proto.FetchRespTopic{
			proto.FetchRespTopic{
				Name: "foo",
				Partitions: []proto.FetchRespPartition{
					proto.FetchRespPartition{
						ID:        1,
						Err:       nil,
						TipOffset: 20,
						Messages:  messages,
					},
				},
			},
			proto.FetchRespTopic{
				Name: "bar",
				Partitions: []proto.FetchRespPartition{
					proto.FetchRespPartition{
						ID:        6,
						Err:       proto.ErrUnknownTopicOrPartition,
						TipOffset: -1,
						Messages:  []*proto.Message{},
					},
				},
			},
		},
	}
	ln, err := testServer(resp1)
	if err != nil {
		t.Fatalf("test server error: %s", err)
	}
	conn, err := NewConnection(ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("could not conect to test server: %s", err)
	}
	resp, err := conn.Fetch(&proto.FetchReq{
		ClientID: "tester",
	})
	if err != nil {
		t.Fatalf("could not fetch response: %s", err)
	}
	if !reflect.DeepEqual(resp, resp1) {
		t.Fatalf("expected different response %#v", resp)
	}
}

func TestConnectionOffset(t *testing.T) {
	resp1 := &proto.OffsetResp{
		CorrelationID: 1,
		Topics: []proto.OffsetRespTopic{
			proto.OffsetRespTopic{
				Name: "test",
				Partitions: []proto.OffsetRespPartition{
					proto.OffsetRespPartition{
						ID:      0,
						Offsets: []int64{92, 0},
					},
				},
			},
		},
	}
	ln, err := testServer(resp1)
	if err != nil {
		t.Fatalf("test server error: %s", err)
	}
	conn, err := NewConnection(ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("could not conect to test server: %s", err)
	}
	resp, err := conn.Offset(&proto.OffsetReq{
		ClientID: "tester",
		Topics: []proto.OffsetReqTopic{
			proto.OffsetReqTopic{
				Name: "test",
				Partitions: []proto.OffsetReqPartition{
					proto.OffsetReqPartition{
						ID:         0,
						TimeMs:     -2,
						MaxOffsets: 2,
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("could not fetch response: %s", err)
	}
	if !reflect.DeepEqual(resp, resp1) {
		t.Fatalf("expected different response %#v", resp)
	}
}

func TestConnectionProduceNoAck(t *testing.T) {
	ln, err := testServer()
	if err != nil {
		t.Fatalf("test server error: %s", err)
	}
	conn, err := NewConnection(ln.Addr().String(), time.Second)
	if err != nil {
		t.Fatalf("could not conect to test server: %s", err)
	}
	resp, err := conn.Produce(&proto.ProduceReq{
		ClientID:     "tester",
		RequiredAcks: proto.RequiredAcksNone,
		Timeout:      time.Second,
		Topics: []proto.ProduceReqTopic{
			proto.ProduceReqTopic{
				Name: "first",
				Partitions: []proto.ProduceReqPartition{
					proto.ProduceReqPartition{
						ID: 0,
						Messages: []*proto.Message{
							&proto.Message{Key: []byte("key 1"), Value: []byte("value 1")},
							&proto.Message{Key: []byte("key 2"), Value: []byte("value 2")},
						},
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("could not fetch response: %s", err)
	}
	if resp != nil {
		t.Fatalf("expected no response, got %#v", resp)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("could not close kafka connection: %s", err)
	}
	if err := ln.Close(); err != nil {
		t.Fatalf("could not close test server: %s", err)
	}
}
