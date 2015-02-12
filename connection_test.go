package kafka

import (
	"net"
	"reflect"
	"testing"
	"time"
)

func testServer(responses [][]byte) (net.Listener, error) {
	ln, err := net.Listen("tcp", "")
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			cli, err := ln.Accept()
			if err != nil {
				return
			}
			for _, resp := range responses {
				cli.Write(resp)
			}
		}
	}()
	return ln, nil
}

func TestConnectionMetadata(t *testing.T) {
	resp1 := &MetadataResp{
		CorrelationID: 1,
		Brokers: []MetadataRespBroker{
			MetadataRespBroker{
				NodeID: 666,
				Host:   "example.com",
				Port:   999,
			},
		},
		Topics: []MetadataRespTopic{
			MetadataRespTopic{
				Name: "foo",
				Partitions: []MetadataRespPartition{
					MetadataRespPartition{
						ID:       7,
						Leader:   7,
						Replicas: []int32{7},
						Isrs:     []int32{7},
					},
				},
			},
		},
	}
	resp1b, err := resp1.Bytes()
	if err != nil {
		t.Fatalf("could not create response message: %s", err)
	}
	ln, err := testServer([][]byte{resp1b})
	if err != nil {
		t.Fatalf("test server error: %s", err)
	}
	conn, err := newConnection(ln.Addr().String())
	if err != nil {
		t.Fatalf("could not conect to test server: %s", err)
	}
	resp, err := conn.Metadata(&MetadataReq{
		ClientID: "tester",
		Topics:   []string{"first", "second"},
	})
	if err != nil {
		t.Fatalf("could not fetch response: %s", err)
	}
	if !reflect.DeepEqual(resp, resp1) {
		t.Fatal("expected different response %#v", resp)
	}
	if err := conn.Close(); err != nil {
		t.Fatalf("could not close kafka connection: %s", err)
	}
	if err := ln.Close(); err != nil {
		t.Fatalf("could not close test server: %s", err)
	}
}

func TestConnectionProduce(t *testing.T) {
	resp1 := &ProduceResp{
		CorrelationID: 1,
		Topics: []ProduceRespTopic{
			ProduceRespTopic{
				Name: "first",
				Partitions: []ProduceRespPartition{
					ProduceRespPartition{
						ID:     0,
						Err:    nil,
						Offset: 4,
					},
				},
			},
		},
	}
	resp1b, err := resp1.Bytes()
	if err != nil {
		t.Fatalf("could not create response message: %s", err)
	}
	resp2 := &ProduceResp{
		CorrelationID: 2,
		Topics: []ProduceRespTopic{
			ProduceRespTopic{
				Name: "first",
				Partitions: []ProduceRespPartition{
					ProduceRespPartition{
						ID:     0,
						Err:    ErrLeaderNotAvailable,
						Offset: -1,
					},
				},
			},
		},
	}
	resp2b, err := resp2.Bytes()
	if err != nil {
		t.Fatalf("could not create response message: %s", err)
	}

	// sending in random order should work as well
	ln, err := testServer([][]byte{resp2b, resp1b})
	if err != nil {
		t.Fatalf("test server error: %s", err)
	}
	conn, err := newConnection(ln.Addr().String())
	if err != nil {
		t.Fatalf("could not conect to test server: %s", err)
	}
	resp, err := conn.Produce(&ProduceReq{
		ClientID:     "tester",
		RequiredAcks: RequiredAcksAll,
		Timeout:      time.Second,
		Topics: []ProduceReqTopic{
			ProduceReqTopic{
				Name: "first",
				Partitions: []ProduceReqPartition{
					ProduceReqPartition{
						ID: 0,
						Messages: []*Message{
							&Message{Key: []byte("key 1"), Value: []byte("value 1")},
							&Message{Key: []byte("key 2"), Value: []byte("value 2")},
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
		t.Fatal("expected different response %#v", resp)
	}
	resp, err = conn.Produce(&ProduceReq{
		ClientID:     "tester",
		RequiredAcks: RequiredAcksAll,
		Timeout:      time.Second,
		Topics: []ProduceReqTopic{
			ProduceReqTopic{
				Name: "first",
				Partitions: []ProduceReqPartition{
					ProduceReqPartition{
						ID: 0,
						Messages: []*Message{
							&Message{Key: []byte("key"), Value: []byte("value")},
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
		t.Fatal("expected different response %#v", resp)
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("could not close kafka connection: %s", err)
	}
	if err := ln.Close(); err != nil {
		t.Fatalf("could not close test server: %s", err)
	}
}

func TestConnectionFetch(t *testing.T) {
	resp1 := &FetchResp{
		CorrelationID: 1,
		Topics: []FetchRespTopic{
			FetchRespTopic{
				Name: "foo",
				Partitions: []FetchRespPartition{
					FetchRespPartition{
						ID:        1,
						Err:       nil,
						TipOffset: 20,
						Messages: []*Message{
							&Message{Offset: 4, Crc: 421, Key: []byte("f"), Value: []byte("first")},
							&Message{Offset: 5, Crc: 921, Key: []byte("s"), Value: []byte("second message")},
						},
					},
				},
			},
			FetchRespTopic{
				Name: "bar",
				Partitions: []FetchRespPartition{
					FetchRespPartition{
						ID:        6,
						Err:       ErrUnknownTopicOrPartition,
						TipOffset: -1,
						Messages:  []*Message{},
					},
				},
			},
		},
	}
	resp1b, err := resp1.Bytes()
	if err != nil {
		t.Fatalf("could not create response message: %s", err)
	}
	// sending in random order should work as well
	ln, err := testServer([][]byte{resp1b})
	if err != nil {
		t.Fatalf("test server error: %s", err)
	}
	conn, err := newConnection(ln.Addr().String())
	if err != nil {
		t.Fatalf("could not conect to test server: %s", err)
	}
	resp, err := conn.Fetch(&FetchReq{
		ClientID: "tester",
	})
	if err != nil {
		t.Fatalf("could not fetch response: %s", err)
	}
	if !reflect.DeepEqual(resp, resp1) {
		t.Fatal("expected different response %#v", resp)
	}
}
