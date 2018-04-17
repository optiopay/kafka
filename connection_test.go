package kafka

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"net"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/optiopay/kafka/proto"
)

const TLSCaFile = "./testkeys/ca.crt"
const TLSCertFile = "./testkeys/oats.crt"
const TLSKeyFile = "./testkeys/oats.key"

type serializableMessage interface {
	Bytes() ([]byte, error)
}

type TLSConf struct {
	ca   []byte
	cert []byte
	key  []byte
}

func getTLSConf() (*TLSConf, error) {
	ca, err := ioutil.ReadFile(TLSCaFile)
	if err != nil {
		return nil, fmt.Errorf("Cannot read %s", TLSCaFile)
	}
	cert, err := ioutil.ReadFile(TLSCertFile)
	if err != nil {
		return nil, fmt.Errorf("Cannot read %s", TLSCertFile)
	}

	key, err := ioutil.ReadFile(TLSKeyFile)
	if err != nil {
		return nil, fmt.Errorf("Cannot read %s", TLSKeyFile)
	}

	return &TLSConf{ca: ca, cert: cert, key: key}, nil

}

//just read request before start to response
func readRequest(r io.Reader) error {
	dec := proto.NewDecoder(r)
	size := dec.DecodeInt32()
	var read int32 = 0
	buf := make([]byte, size)

	for read < size {
		n, err := r.Read(buf)
		if err != nil {
			return err
		}
		read += int32(n)
	}
	return nil
}

func testTLSServer(messages ...serializableMessage) (net.Listener, error) {
	tlsConf, err := getTLSConf()
	if err != nil {
		return nil, err
	}

	roots := x509.NewCertPool()
	ok := roots.AppendCertsFromPEM(tlsConf.ca)
	if !ok {
		return nil, fmt.Errorf("Cannot parse root certificate")
	}

	certificate, err := tls.X509KeyPair(tlsConf.cert, tlsConf.key)
	if err != nil {
		return nil, fmt.Errorf("Failed to parse key/cert for TLS: %s", err)
	}

	conf := &tls.Config{
		Certificates: []tls.Certificate{certificate},
		RootCAs:      roots,
	}

	_ = conf

	ln, err := tls.Listen("tcp4", "localhost:22222", conf)
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
					err := readRequest(conn)
					if err != nil {
						log.Panic(err)
					}
					_, _ = cli.Write(resp)
				}
				err = cli.Close()
			}(cli)
		}
	}()
	return ln, nil
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

	msgs := make(chan serializableMessage, 10)

	go func() {
		for {
			cli, err := ln.Accept()
			if err != nil {
				return
			}

			go func(conn net.Conn) {
				defer func() { _ = cli.Close() }()

				for msg := range msgs {
					err := readRequest(conn)
					if err != nil {
						log.Panic(err)
					}
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

	ch <- versionResp
	conn, err := newTCPConnection(ln.Addr().String(), time.Second, time.Second)
	if err != nil {
		t.Fatalf("could not connect to test server: %s", err)
	}
	ch <- resp1
	resp, err := conn.Metadata(&proto.MetadataReq{
		RequestHeader: proto.RequestHeader{ClientID: "tester"},
		Topics:        []string{"first", "second"},
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
		t.Fatalf("could not connect to test server: %s", err)
	}

	go func() {
		time.Sleep(time.Millisecond * 10)
		msgs <- resp1

		time.Sleep(time.Millisecond * 10)
		msgs <- resp2

	}()

	resp, err := conn.Produce(&proto.ProduceReq{
		RequestHeader: proto.RequestHeader{ClientID: "tester"},
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
		RequestHeader: proto.RequestHeader{ClientID: "tester"},
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
						Messages:  nil,
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
		t.Fatalf("could not connect to test server: %s", err)
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		ch <- resp1
	}()

	resp, err := conn.Fetch(&proto.FetchReq{
		RequestHeader: proto.RequestHeader{ClientID: "tester"},
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
		t.Fatalf("could not connect to test server: %s", err)
	}

	go func() {
		time.Sleep(50 * time.Millisecond)
		ch <- resp1
	}()

	resp, err := conn.Offset(&proto.OffsetReq{
		RequestHeader: proto.RequestHeader{ClientID: "tester"},
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

func TestOffsetResponseWithVersions(t *testing.T) {

	resp0 := proto.OffsetResp{
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

	b, err := resp0.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	r0, err := proto.ReadVersionedOffsetResp(bytes.NewReader(b), resp0.Version)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(resp0, *r0) {
		t.Fatalf("expected different response \n %#v  expected \n %#v", resp0, r0)
	}

	resp1 := resp0
	resp1.Version = proto.KafkaV1

	ts := time.Unix(0, (time.Now().UnixNano()/int64(time.Millisecond))*int64(time.Millisecond))
	resp1.Topics[0].Partitions[0].TimeStamp = ts

	// In kafka >= KafkaV1 there might be only one offset
	resp1.Topics[0].Partitions[0].Offsets = []int64{92}

	b1, err := resp1.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	r1, err := proto.ReadVersionedOffsetResp(bytes.NewReader(b1), resp1.Version)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(resp1, *r1) {
		t.Fatalf("expected different response \n %#v  expected \n %#v", resp1, *r1)
	}

	resp2 := resp1
	resp2.Version = proto.KafkaV2
	resp2.ThrottleTime = 2 * time.Second

	b2, err := resp2.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	r2, err := proto.ReadVersionedOffsetResp(bytes.NewReader(b2), resp2.Version)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(resp2, *r2) {
		t.Fatalf("expected different response \n %#v  expected \n %#v", resp2, *r2)
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
		t.Fatalf("could not connect to test server: %s", err)
	}
	resp, err := conn.Produce(&proto.ProduceReq{
		RequestHeader: proto.RequestHeader{ClientID: "tester"},
		Compression:   proto.CompressionNone,
		RequiredAcks:  proto.RequiredAcksNone,
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

func TestClosedConnectionWriter(t *testing.T) {
	// create test server with no messages, so that any client connection will
	// be immediately closed
	ln, err := testServer()
	if err != nil {
		t.Fatalf("test server error: %s", err)
	}
	conn, err := newTCPConnection(ln.Addr().String(), time.Second, time.Second)
	if err != nil {
		t.Fatalf("could not connect to test server: %s", err)
	}

	longBytes := []byte(strings.Repeat("xxxxxxxxxxxxxxxxxxxxxx", 1000))
	req := proto.ProduceReq{
		RequestHeader: proto.RequestHeader{ClientID: "test-client"},
		Compression:   proto.CompressionNone,
		RequiredAcks:  proto.RequiredAcksAll,
		Timeout:       100,
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
		t.Fatalf("could not connect to test server: %s", err)
	}

	req := &proto.FetchReq{
		RequestHeader: proto.RequestHeader{ClientID: "test-client"},
		MaxWaitTime:   100,
		MinBytes:      0,
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
		t.Fatalf("could not connect to test server: %s", err)
	}

	req := &proto.FetchReq{
		RequestHeader: proto.RequestHeader{ClientID: "test-client"},
		MaxWaitTime:   100,
		MinBytes:      0,
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
		t.Fatalf("could not connect to test server: %s", err)
	}
	_, err = conn.Metadata(&proto.MetadataReq{
		RequestHeader: proto.RequestHeader{ClientID: "tester"},
		Topics:        []string{"first", "second"},
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

func TestTLSConnection(t *testing.T) {
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
	ln, err := testTLSServer(versionResp, resp1)
	if err != nil {
		t.Fatalf("test server error: %s", err)
	}
	tlsConf, err := getTLSConf()
	if err != nil {
		t.Fatalf("cannot get tls parametes: %s", err)
	}
	_ = tlsConf
	conn, err := newTLSConnection(ln.Addr().String(), tlsConf.ca, tlsConf.cert, tlsConf.key, time.Second, time.Second)

	if err != nil {
		t.Fatalf("could not connect to test server: %s", err)
	}
	resp, err := conn.Metadata(&proto.MetadataReq{
		RequestHeader: proto.RequestHeader{ClientID: "tester"},
		Topics:        []string{"first", "second"},
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
