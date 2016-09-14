package kafka

import (
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/optiopay/kafka/proto"
)

type serializableMessage interface {
	Bytes() ([]byte, error)
}

func testServer(messages ...serializableMessage) (net.Listener, error) {
	ln, err := net.Listen("tcp4", "")
	if err != nil {
		return nil, err
	}

	responses := make([][]byte, len(messages))
	for i, m := range messages {
		b, err := m.Bytes()
		if err != nil {
			_ = ln.Close()
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

				time.Sleep(time.Millisecond * 50)
				for _, resp := range responses {
					_, _ = cli.Write(resp)
				}
				_ = cli.Close()
			}(cli)
		}
	}()
	return ln, nil
}

func testServer2() (net.Listener, chan serializableMessage, error) {
	ln, err := net.Listen("tcp4", "")
	if err != nil {
		return nil, nil, err
	}

	msgs := make(chan serializableMessage)

	go func() {
		for {
			cli, err := ln.Accept()
			if err != nil {
				return
			}

			go func(conn net.Conn) {
				defer func() { _ = cli.Close() }()

				for msg := range msgs {
					b, err := msg.Bytes()
					if err != nil {
						panic(err)
					}
					if _, err = cli.Write(b); err != nil {
						return
					}
				}
			}(cli)
		}
	}()
	return ln, msgs, nil
}

func testServer3() (net.Listener, error) {
	ln, err := net.Listen("tcp4", "")
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			cli, err := ln.Accept()
			if err != nil {
				return
			}

			go func(conn net.Conn) {
				_, _ = cli.Read(make([]byte, 1024))
				_ = cli.Close()
			}(cli)
		}
	}()
	return ln, nil
}

func testSilentServer() (net.Listener, error) {
	ln, err := net.Listen("tcp4", "")
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			cli, err := ln.Accept()
			if err != nil {
				return
			}

			go func(conn net.Conn) {
				_, _ = cli.Read(make([]byte, 1024))
			}(cli)
		}
	}()
	return ln, nil
}

func TestConnectionMetadata(t *testing.T) {
	versionResp := &proto.APIVersionsResp{
		CorrelationID: 1,
	}
	resp1 := &proto.MetadataResp{
		CorrelationID: 2,
		Brokers: []proto.MetadataRespBroker{
			{
				NodeID: 666,
				Host:   "example.com",
				Port:   999,
			},
		},
		Topics: []proto.MetadataRespTopic{
			{
				Name: "foo",
				Partitions: []proto.MetadataRespPartition{
					{
						ID:       7,
						Leader:   7,
						Replicas: []int32{7},
						Isrs:     []int32{7},
					},
				},
			},
		},
	}
	ln, ch, err := testServer2()
	if err != nil {
		t.Fatalf("test server error: %s", err)
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		ch <- versionResp
	}()

	conn, err := newTCPConnection(ln.Addr().String(), time.Second, time.Second)
	if err != nil {
		t.Fatalf("could not conect to test server: %s", err)
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		ch <- resp1
	}()
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
	versionResp := &proto.APIVersionsResp{
		CorrelationID: 1,
	}
	resp1 := &proto.ProduceResp{
		CorrelationID: 2,
		Topics: []proto.ProduceRespTopic{
			{
				Name: "first",
				Partitions: []proto.ProduceRespPartition{
					{
						ID:     0,
						Err:    nil,
						Offset: 4,
					},
				},
			},
		},
	}
	resp2 := &proto.ProduceResp{
		CorrelationID: 3,
		Topics: []proto.ProduceRespTopic{
			{
				Name: "first",
				Partitions: []proto.ProduceRespPartition{
					{
						ID:     0,
						Err:    proto.ErrLeaderNotAvailable,
						Offset: -1,
					},
				},
			},
		},
	}

	ln, msgs, err := testServer2()
	if err != nil {
		t.Fatalf("test server error: %s", err)
	}
	go func() {
		time.Sleep(time.Millisecond * 10)
		msgs <- versionResp

	}()
	conn, err := newTCPConnection(ln.Addr().String(), time.Second, time.Second)
	if err != nil {
		t.Fatalf("could not conect to test server: %s", err)
	}

	go func() {
		time.Sleep(time.Millisecond * 10)
		msgs <- resp1

		time.Sleep(time.Millisecond * 10)
		msgs <- resp2

	}()

	resp, err := conn.Produce(&proto.ProduceReq{
		CorrelationID: 1,
		ClientID:      "tester",
		Compression:   proto.CompressionNone,
		RequiredAcks:  proto.RequiredAcksAll,
		Timeout:       time.Second,
		Topics: []proto.ProduceReqTopic{
			{
				Name: "first",
				Partitions: []proto.ProduceReqPartition{
					{
						ID: 0,
						Messages: []*proto.Message{
							{Key: []byte("key 1"), Value: []byte("value 1")},
							{Key: []byte("key 2"), Value: []byte("value 2")},
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
		CorrelationID: 2,
		ClientID:      "tester",
		Compression:   proto.CompressionNone,
		RequiredAcks:  proto.RequiredAcksAll,
		Timeout:       time.Second,
		Topics: []proto.ProduceReqTopic{
			{
				Name: "first",
				Partitions: []proto.ProduceReqPartition{
					{
						ID: 0,
						Messages: []*proto.Message{
							{Key: []byte("key"), Value: []byte("value")},
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
	versionResp := &proto.APIVersionsResp{
		CorrelationID: 1,
	}

	messages := []*proto.Message{
		{Offset: 4, Key: []byte("f"), Value: []byte("first"), TipOffset: 20},
		{Offset: 5, Key: []byte("s"), Value: []byte("second"), TipOffset: 20},
		{Offset: 6, Key: []byte("t"), Value: []byte("third"), TipOffset: 20},
	}
	for _, m := range messages {
		m.Crc = proto.ComputeCrc(m, proto.CompressionNone)
	}

	resp1 := &proto.FetchResp{
		CorrelationID: 2,
		Topics: []proto.FetchRespTopic{
			{
				Name: "foo",
				Partitions: []proto.FetchRespPartition{
					{
						ID:        1,
						Err:       nil,
						TipOffset: 20,
						Messages:  messages,
					},
				},
			},
			{
				Name: "bar",
				Partitions: []proto.FetchRespPartition{
					{
						ID:        6,
						Err:       proto.ErrUnknownTopicOrPartition,
						TipOffset: -1,
						Messages:  []*proto.Message{},
					},
				},
			},
		},
	}
	ln, ch, err := testServer2()
	if err != nil {
		t.Fatalf("test server error: %s", err)
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		ch <- versionResp
	}()
	conn, err := newTCPConnection(ln.Addr().String(), time.Second, time.Second)
	if err != nil {
		t.Fatalf("could not conect to test server: %s", err)
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		ch <- resp1
	}()

	resp, err := conn.Fetch(&proto.FetchReq{
		CorrelationID: 2,
		ClientID:      "tester",
		Topics: []proto.FetchReqTopic{
			{
				Name: "foo",
				Partitions: []proto.FetchReqPartition{
					{
						ID:          1,
						FetchOffset: 5,
					},
				},
			},
			{
				Name: "bar",
				Partitions: []proto.FetchReqPartition{
					{
						ID: 6,
					},
				},
			},
		},
	})
	if err != nil {
		t.Fatalf("could not fetch response: %s", err)
	}

	// before comparison, set attributes as we expect deserializer to do
	for _, m := range messages {
		m.Topic = "foo"
		m.Partition = 1
	}
	// offset 5 was requested; first message should be trimmed
	resp1.Topics[0].Partitions[0].Messages = messages[1:]

	if !reflect.DeepEqual(resp, resp1) {
		t.Fatalf("expected different response %#v", resp)
	}
}

func TestConnectionOffset(t *testing.T) {
	versionResp := &proto.APIVersionsResp{
		CorrelationID: 1,
	}

	resp1 := &proto.OffsetResp{
		CorrelationID: 2,
		Topics: []proto.OffsetRespTopic{
			{
				Name: "test",
				Partitions: []proto.OffsetRespPartition{
					{
						ID:      0,
						Offsets: []int64{92, 0},
					},
				},
			},
		},
	}

	ln, ch, err := testServer2()
	if err != nil {
		t.Fatalf("test server error: %s", err)
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		ch <- versionResp
	}()

	conn, err := newTCPConnection(ln.Addr().String(), time.Second, time.Second)
	if err != nil {
		t.Fatalf("could not conect to test server: %s", err)
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		ch <- resp1
	}()

	resp, err := conn.Offset(&proto.OffsetReq{
		ClientID: "tester",
		Topics: []proto.OffsetReqTopic{
			{
				Name: "test",
				Partitions: []proto.OffsetReqPartition{
					{
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
	versionResp := &proto.APIVersionsResp{
		CorrelationID: 1,
	}
	ln, ch, err := testServer2()
	if err != nil {
		t.Fatalf("test server error: %s", err)
	}
	go func() {
		time.Sleep(50 * time.Millisecond)
		ch <- versionResp
	}()
	conn, err := newTCPConnection(ln.Addr().String(), time.Second, time.Second)
	if err != nil {
		t.Fatalf("could not conect to test server: %s", err)
	}
	resp, err := conn.Produce(&proto.ProduceReq{
		ClientID:     "tester",
		Compression:  proto.CompressionNone,
		RequiredAcks: proto.RequiredAcksNone,
		Timeout:      time.Second,
		Topics: []proto.ProduceReqTopic{
			{
				Name: "first",
				Partitions: []proto.ProduceReqPartition{
					{
						ID: 0,
						Messages: []*proto.Message{
							{Key: []byte("key 1"), Value: []byte("value 1")},
							{Key: []byte("key 2"), Value: []byte("value 2")},
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

func TestConnectionProduceWithVersions(t *testing.T) {

	srv := NewServer()
	srv.Start()
	defer srv.Close()

	var apiVersionHandler RequestHandler

	apiVersionHandler = func(request Serializable) Serializable {
		req := request.(*proto.APIVersionsReq)
		return &proto.APIVersionsResp{
			CorrelationID: req.CorrelationID,
			APIVersions: []proto.SupportedVersion{
				proto.SupportedVersion{APIKey: proto.ProduceReqKind, MinVersion: 0, MaxVersion: 1},
			},
		}
	}

	srv.Handle(proto.APIVersionsReqKind, apiVersionHandler)
	srv.Handle(proto.MetadataReqKind, NewMetadataHandler(srv, true).Handler())

	conn, err := newTCPConnection(srv.Address(), time.Second, time.Second)
	if err != nil {
		t.Fatalf("could not conect to test server: %s", err)
	}

	req := proto.ProduceReq{
		ClientID:     "tester",
		Compression:  proto.CompressionNone,
		RequiredAcks: proto.RequiredAcksAll,
		Timeout:      time.Second,
		Topics: []proto.ProduceReqTopic{
			{
				Name: "first",
				Partitions: []proto.ProduceReqPartition{
					{
						ID: 0,
						Messages: []*proto.Message{
							{Key: []byte("key 1"), Value: []byte("value 1")},
							{Key: []byte("key 2"), Value: []byte("value 2")},
						},
					},
				},
			},
		},
	}

	//Version 0

	resp, err := conn.Produce(&req)
	if err != nil {
		t.Fatalf("could not fetch response: %s", err)
	}

	if resp == nil {
		t.Fatal("expected response, got nil")
	}
	if resp.Version != req.Version {
		t.Fatalf("Version mismatch should be %s, got: %s", req.Version, resp.Version)
	}

	if resp.ThrottleTimeMs != 0 {
		t.Fatalf("For version 0 ThrottleTimeMs should be 0, got: %s", resp.ThrottleTimeMs)
	}
	if resp.Topics[0].Partitions[0].Timestamp != 0 {
		t.Fatalf("For version 0 Timestamp should be 0, got: %s", resp.Topics[0].Partitions[0].Timestamp)
	}

	// Version 1
	req.Version = 1
	resp, err = conn.Produce(&req)
	if err != nil {
		t.Fatalf("could not fetch response: %s", err)
	}

	if resp == nil {
		t.Fatal("expected response, got nil")
	}

	if resp.Version != req.Version {
		t.Fatalf("Version mismatch should be %s, got: %s", req.Version, resp.Version)
	}

	if resp.ThrottleTimeMs != 3 {
		t.Fatalf("For version 1 ThrottleTimeMs should be 3, got: %s", resp.ThrottleTimeMs)
	}

	if resp.Topics[0].Partitions[0].Timestamp != 0 {
		t.Fatalf("For version 1 Timestamp should be 0, got: %s", resp.Topics[0].Partitions[0].Timestamp)
	}

	// Version 2

	req.Version = 2
	resp, err = conn.Produce(&req)
	if err != nil {
		t.Fatalf("could not fetch response: %s", err)
	}

	if resp == nil {
		t.Fatal("expected response, got nil")
	}

	if resp.Version != req.Version {
		t.Fatalf("Version mismatch should be %s, got: %s", req.Version, resp.Version)
	}

	if resp.ThrottleTimeMs != 3 {
		t.Fatalf("For version 2 ThrottleTimeMs should be 3, got: %s", resp.ThrottleTimeMs)
	}

	if resp.Topics[0].Partitions[0].Timestamp == 0 {
		t.Fatal("For version 2 Timestamp should not be 0")
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("could not close kafka connection: %s", err)
	}
}

func TestClosedConnectionWriter(t *testing.T) {
	// create test server with no messages, so that any client connection will
	// be immediately closed
	ln, err := testServer()
	if err != nil {
		t.Fatalf("test server error: %s", err)
	}
	conn, err := newTCPConnection(ln.Addr().String(), time.Second, time.Second)
	if err != nil {
		t.Fatalf("could not conect to test server: %s", err)
	}

	longBytes := []byte(strings.Repeat("xxxxxxxxxxxxxxxxxxxxxx", 1000))
	req := proto.ProduceReq{
		ClientID:     "test-client",
		Compression:  proto.CompressionNone,
		RequiredAcks: proto.RequiredAcksAll,
		Timeout:      100,
		Topics: []proto.ProduceReqTopic{
			{
				Name: "test-topic",
				Partitions: []proto.ProduceReqPartition{
					{
						ID: 0,
						Messages: []*proto.Message{
							{Value: longBytes},
						},
					},
				},
			},
		},
	}
	for i := 0; i < 10; i++ {
		if _, err := conn.Produce(&req); err == nil {
			t.Fatal("message publishing after closing connection should not be possible")
		}
	}

	// although we produced ten requests, because connection is closed, no
	// response channel should be registered
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if len(conn.respc) != 0 {
		t.Fatalf("expected 0 waiting responses, got %d", len(conn.respc))
	}
}

func TestClosedConnectionReader(t *testing.T) {
	// create test server with no messages, so that any client connection will
	// be immediately closed
	ln, err := testServer()
	if err != nil {
		t.Fatalf("test server error: %s", err)
	}
	conn, err := newTCPConnection(ln.Addr().String(), time.Second, time.Second)
	if err != nil {
		t.Fatalf("could not conect to test server: %s", err)
	}

	req := &proto.FetchReq{
		ClientID:    "test-client",
		MaxWaitTime: 100,
		MinBytes:    0,
		Topics: []proto.FetchReqTopic{
			{
				Name: "my-topic",
				Partitions: []proto.FetchReqPartition{
					{
						ID:          0,
						FetchOffset: 1,
						MaxBytes:    100000,
					},
				},
			},
		},
	}

	for i := 0; i < 10; i++ {
		if _, err := conn.Fetch(req); err == nil {
			t.Fatal("fetching from closed connection succeeded")
		}
	}

	// although we produced ten requests, because connection is closed, no
	// response channel should be registered
	conn.mu.Lock()
	defer conn.mu.Unlock()
	if len(conn.respc) != 0 {
		t.Fatalf("expected 0 waiting responses, got %d", len(conn.respc))
	}
}

func TestConnectionReaderAfterEOF(t *testing.T) {
	ln, err := testServer3()
	if err != nil {
		t.Fatalf("test server error: %s", err)
	}
	defer func() {
		_ = ln.Close()
	}()

	conn, err := newTCPConnection(ln.Addr().String(), time.Second, time.Second)
	if err != nil {
		t.Fatalf("could not conect to test server: %s", err)
	}

	req := &proto.FetchReq{
		ClientID:    "test-client",
		MaxWaitTime: 100,
		MinBytes:    0,
		Topics: []proto.FetchReqTopic{
			{
				Name: "my-topic",
				Partitions: []proto.FetchReqPartition{
					{
						ID:          0,
						FetchOffset: 1,
						MaxBytes:    100000,
					},
				},
			},
		},
	}

	if _, err := conn.Fetch(req); err == nil {
		t.Fatal("fetching from closed connection succeeded")
	}

	// Wait until testServer3 closes connection
	time.Sleep(time.Millisecond * 50)

	if _, err := conn.Fetch(req); err == nil {
		t.Fatal("fetching from closed connection succeeded")
	}
}

func TestNoServerResponse(t *testing.T) {
	ln, err := testSilentServer()
	if err != nil {
		t.Fatalf("test server error: %s", err)
	}
	conn, err := newTCPConnection(ln.Addr().String(), time.Second, time.Second)
	if err != nil {
		t.Fatalf("could not conect to test server: %s", err)
	}
	_, err = conn.Metadata(&proto.MetadataReq{
		ClientID: "tester",
		Topics:   []string{"first", "second"},
	})
	if err == nil {
		t.Fatalf("expected timeout error, did not happen")
	}

	if err := conn.Close(); err != nil {
		t.Fatalf("could not close kafka connection: %s", err)
	}
	if err := ln.Close(); err != nil {
		t.Fatalf("could not close test server: %s", err)
	}
}
