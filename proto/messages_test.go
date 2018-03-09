package proto

import (
	"bytes"
	"io"
	"reflect"
	"testing"
	"time"
)

type Request interface {
	Bytes() ([]byte, error)
	WriteTo(w io.Writer) (int64, error)
}

var _ Request = &MetadataReq{}
var _ Request = &ProduceReq{}
var _ Request = &FetchReq{}
var _ Request = &ConsumerMetadataReq{}
var _ Request = &OffsetReq{}
var _ Request = &OffsetCommitReq{}
var _ Request = &OffsetFetchReq{}
var _ Request = &APIVersionsReq{}

func testRequestSerialization(t *testing.T, r Request) {
	var buf bytes.Buffer
	if n, err := r.WriteTo(&buf); err != nil {
		t.Fatalf("could not write request to buffer: %s", err)
	} else if n != int64(buf.Len()) {
		t.Fatalf("writer returned invalid number of bytes written %d != %d", n, buf.Len())
	}
	b, err := r.Bytes()
	if err != nil {
		t.Fatalf("could not convert request to bytes: %s", err)
	}
	if !bytes.Equal(b, buf.Bytes()) {
		t.Fatal("Bytes() and WriteTo() serialized request is of different form")
	}
}

func TestMetadataRequest(t *testing.T) {
	req1 := &MetadataReq{
		CorrelationID: 123,
		ClientID:      "testcli",
		Topics:        nil,
		Version:       KafkaV0,
	}
	testRequestSerialization(t, req1)
	b, _ := req1.Bytes()
	expected := []byte{0x0, 0x0, 0x0, 0x15, 0x0, 0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x7b, 0x0, 0x7, 0x74, 0x65, 0x73, 0x74, 0x63, 0x6c, 0x69, 0x0, 0x0, 0x0, 0x0}

	if !bytes.Equal(b, expected) {
		t.Fatalf("expected different bytes representation: %v", b)
	}

	req2 := &MetadataReq{
		CorrelationID: 123,
		ClientID:      "testcli",
		Topics:        []string{"foo", "bar"},
	}
	testRequestSerialization(t, req2)
	b, _ = req2.Bytes()
	expected = []byte{0x0, 0x0, 0x0, 0x1f, 0x0, 0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x7b, 0x0, 0x7, 0x74, 0x65, 0x73, 0x74, 0x63, 0x6c, 0x69, 0x0, 0x0, 0x0, 0x2, 0x0, 0x3, 0x66, 0x6f, 0x6f, 0x0, 0x3, 0x62, 0x61, 0x72}

	if !bytes.Equal(b, expected) {
		t.Fatalf("expected different bytes representation: %v", b)
	}

	r, _ := ReadMetadataReq(bytes.NewBuffer(expected))
	if !reflect.DeepEqual(r, req2) {
		t.Fatalf("malformed request: %#v", r)
	}

	req3 := &MetadataReq{
		CorrelationID:          123,
		ClientID:               "testcli",
		Topics:                 nil,
		Version:                KafkaV4,
		AllowAutoTopicCreation: true,
	}
	testRequestSerialization(t, req3)
	b3, _ := req3.Bytes()
	expected3 := []byte{0x0, 0x0, 0x0, 0x16, 0x0, 0x3, 0x0, 0x4, 0x0, 0x0, 0x0, 0x7b, 0x0, 0x7, 0x74, 0x65, 0x73, 0x74, 0x63, 0x6c, 0x69, 0xFF, 0xFF, 0xFF, 0xFF, 0x1}

	if !bytes.Equal(b3, expected3) {
		t.Fatalf("expected different bytes representation: %v ( expected %v)", b3, expected3)
	}
}

func TestMetadataResponse(t *testing.T) {
	msgb := []byte{0x0, 0x0, 0x1, 0xc7, 0x0, 0x0, 0x0, 0x7b, 0x0, 0x0, 0x0, 0x4, 0x0, 0x0, 0xc0, 0x10, 0x0, 0xb, 0x31, 0x37, 0x32, 0x2e, 0x31, 0x37, 0x2e, 0x34, 0x32, 0x2e, 0x31, 0x0, 0x0, 0xc0, 0x10, 0x0, 0x0, 0xc0, 0x12, 0x0, 0xb, 0x31, 0x37, 0x32, 0x2e, 0x31, 0x37, 0x2e, 0x34, 0x32, 0x2e, 0x31, 0x0, 0x0, 0xc0, 0x12, 0x0, 0x0, 0xc0, 0x11, 0x0, 0xb, 0x31, 0x37, 0x32, 0x2e, 0x31, 0x37, 0x2e, 0x34, 0x32, 0x2e, 0x31, 0x0, 0x0, 0xc0, 0x11, 0x0, 0x0, 0xc0, 0x13, 0x0, 0xb, 0x31, 0x37, 0x32, 0x2e, 0x31, 0x37, 0x2e, 0x34, 0x32, 0x2e, 0x31, 0x0, 0x0, 0xc0, 0x13, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x3, 0x66, 0x6f, 0x6f, 0x0, 0x0, 0x0, 0x6, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0xc0, 0x13, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0xc0, 0x13, 0x0, 0x0, 0xc0, 0x10, 0x0, 0x0, 0xc0, 0x11, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0xc0, 0x13, 0x0, 0x0, 0xc0, 0x10, 0x0, 0x0, 0xc0, 0x11, 0x0, 0x0, 0x0, 0x0, 0x0, 0x5, 0x0, 0x0, 0xc0, 0x12, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0xc0, 0x12, 0x0, 0x0, 0xc0, 0x10, 0x0, 0x0, 0xc0, 0x11, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0xc0, 0x12, 0x0, 0x0, 0xc0, 0x10, 0x0, 0x0, 0xc0, 0x11, 0x0, 0x0, 0x0, 0x0, 0x0, 0x4, 0x0, 0x0, 0xc0, 0x11, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0xc0, 0x11, 0x0, 0x0, 0xc0, 0x13, 0x0, 0x0, 0xc0, 0x10, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0xc0, 0x11, 0x0, 0x0, 0xc0, 0x13, 0x0, 0x0, 0xc0, 0x10, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0xc0, 0x12, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0xc0, 0x12, 0x0, 0x0, 0xc0, 0x13, 0x0, 0x0, 0xc0, 0x10, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0xc0, 0x12, 0x0, 0x0, 0xc0, 0x13, 0x0, 0x0, 0xc0, 0x10, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0xc0, 0x10, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0xc0, 0x10, 0x0, 0x0, 0xc0, 0x11, 0x0, 0x0, 0xc0, 0x12, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0xc0, 0x10, 0x0, 0x0, 0xc0, 0x11, 0x0, 0x0, 0xc0, 0x12, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc0, 0x11, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0xc0, 0x11, 0x0, 0x0, 0xc0, 0x12, 0x0, 0x0, 0xc0, 0x13, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0xc0, 0x11, 0x0, 0x0, 0xc0, 0x12, 0x0, 0x0, 0xc0, 0x13, 0x0, 0x0, 0x0, 0x4, 0x74, 0x65, 0x73, 0x74, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0xc0, 0x11, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0xc0, 0x11, 0x0, 0x0, 0xc0, 0x12, 0x0, 0x0, 0xc0, 0x13, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0xc0, 0x11, 0x0, 0x0, 0xc0, 0x12, 0x0, 0x0, 0xc0, 0x13, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xc0, 0x10, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0xc0, 0x10, 0x0, 0x0, 0xc0, 0x11, 0x0, 0x0, 0xc0, 0x12, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0xc0, 0x10, 0x0, 0x0, 0xc0, 0x11, 0x0, 0x0, 0xc0, 0x12}
	resp, err := ReadMetadataResp(bytes.NewBuffer(msgb), 0)
	if err != nil {
		t.Fatalf("could not read metadata response: %s", err)
	}
	expected := &MetadataResp{
		CorrelationID: 123,
		Brokers: []MetadataRespBroker{
			{NodeID: 49168, Host: "172.17.42.1", Port: 49168},
			{NodeID: 49170, Host: "172.17.42.1", Port: 49170},
			{NodeID: 49169, Host: "172.17.42.1", Port: 49169},
			{NodeID: 49171, Host: "172.17.42.1", Port: 49171},
		},
		Topics: []MetadataRespTopic{
			{
				Name: "foo",
				Err:  error(nil),
				Partitions: []MetadataRespPartition{
					{Err: error(nil), ID: 2, Leader: 49171, Replicas: []int32{49171, 49168, 49169}, Isrs: []int32{49171, 49168, 49169}},
					{Err: error(nil), ID: 5, Leader: 49170, Replicas: []int32{49170, 49168, 49169}, Isrs: []int32{49170, 49168, 49169}},
					{Err: error(nil), ID: 4, Leader: 49169, Replicas: []int32{49169, 49171, 49168}, Isrs: []int32{49169, 49171, 49168}},
					{Err: error(nil), ID: 1, Leader: 49170, Replicas: []int32{49170, 49171, 49168}, Isrs: []int32{49170, 49171, 49168}},
					{Err: error(nil), ID: 3, Leader: 49168, Replicas: []int32{49168, 49169, 49170}, Isrs: []int32{49168, 49169, 49170}},
					{Err: error(nil), ID: 0, Leader: 49169, Replicas: []int32{49169, 49170, 49171}, Isrs: []int32{49169, 49170, 49171}},
				},
			},
			{
				Name: "test",
				Err:  error(nil),
				Partitions: []MetadataRespPartition{
					{Err: error(nil), ID: 1, Leader: 49169, Replicas: []int32{49169, 49170, 49171}, Isrs: []int32{49169, 49170, 49171}},
					{Err: error(nil), ID: 0, Leader: 49168, Replicas: []int32{49168, 49169, 49170}, Isrs: []int32{49168, 49169, 49170}},
				},
			},
		},
	}

	if !reflect.DeepEqual(resp, expected) {
		t.Fatalf("expected different message: %#v", resp)
	}

	if b, err := resp.Bytes(); err != nil {
		t.Fatalf("cannot serialize response: %s", err)
	} else {
		if !bytes.Equal(b, msgb) {
			t.Fatalf("serialized representation different from expected: %#v", b)
		}
	}
}
func TestAPIVersionsResponse(t *testing.T) {
	respOrig := &APIVersionsResp{
		CorrelationID: 1,
		APIVersions: []SupportedVersion{
			SupportedVersion{
				APIKey:     1,
				MinVersion: 0,
				MaxVersion: 2,
			},
		},
	}
	b, err := respOrig.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	resp, err := ReadAPIVersionsResp(bytes.NewBuffer(b))
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(respOrig, resp) {
		t.Errorf("Should be equal %+v %+v", respOrig, resp)
	}

}

func TestMetadataResponseVersions(t *testing.T) {
	expectedV1 := MetadataResp{
		Version:       1,
		CorrelationID: 123,
		ControllerID:  5,
		Brokers: []MetadataRespBroker{
			{NodeID: 49168, Host: "172.17.42.1", Port: 49168, Rack: "rack1"},
			{NodeID: 49170, Host: "172.17.42.1", Port: 49170, Rack: "rack1"},
			{NodeID: 49169, Host: "172.17.42.1", Port: 49169, Rack: "rack1"},
			{NodeID: 49171, Host: "172.17.42.1", Port: 49171, Rack: "rack1"},
		},
		Topics: []MetadataRespTopic{
			{
				Name: "foo",
				Err:  error(nil),
				Partitions: []MetadataRespPartition{
					{Err: error(nil), ID: 2, Leader: 49171, Replicas: []int32{49171, 49168, 49169}, Isrs: []int32{49171, 49168, 49169}},
					{Err: error(nil), ID: 5, Leader: 49170, Replicas: []int32{49170, 49168, 49169}, Isrs: []int32{49170, 49168, 49169}},
					{Err: error(nil), ID: 4, Leader: 49169, Replicas: []int32{49169, 49171, 49168}, Isrs: []int32{49169, 49171, 49168}},
					{Err: error(nil), ID: 1, Leader: 49170, Replicas: []int32{49170, 49171, 49168}, Isrs: []int32{49170, 49171, 49168}},
					{Err: error(nil), ID: 3, Leader: 49168, Replicas: []int32{49168, 49169, 49170}, Isrs: []int32{49168, 49169, 49170}},
					{Err: error(nil), ID: 0, Leader: 49169, Replicas: []int32{49169, 49170, 49171}, Isrs: []int32{49169, 49170, 49171}},
				},
				IsInternal: true,
			},
			{
				Name: "test",
				Err:  error(nil),
				Partitions: []MetadataRespPartition{
					{Err: error(nil), ID: 1, Leader: 49169, Replicas: []int32{49169, 49170, 49171}, Isrs: []int32{49169, 49170, 49171}},
					{Err: error(nil), ID: 0, Leader: 49168, Replicas: []int32{49168, 49169, 49170}, Isrs: []int32{49168, 49169, 49170}},
				},
			},
		},
	}

	b, err := expectedV1.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	resp, err := ReadMetadataResp(bytes.NewBuffer(b), expectedV1.Version)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(&expectedV1, resp) {
		t.Fatalf("Different response expectedV1 = %+v, got = %+v", expectedV1, resp)
	}

	expectedV2 := expectedV1
	expectedV2.Version = 2
	expectedV2.ClusterID = "cluster id"

	b, err = expectedV2.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	resp2, err := ReadMetadataResp(bytes.NewBuffer(b), expectedV2.Version)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(&expectedV2, resp2) {
		t.Fatalf("Different response expectedV2 = %+v, got = %+v", expectedV2, resp2)
	}

	expectedV3 := expectedV2
	expectedV3.Version = 3
	expectedV3.ThrottleTime = time.Second

	b, err = expectedV3.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	resp3, err := ReadMetadataResp(bytes.NewBuffer(b), expectedV3.Version)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(&expectedV3, resp3) {
		t.Fatalf("Different response expectedV3 = %+v, got = %+v", expectedV3, resp3)
	}

}

func TestProduceResponse(t *testing.T) {
	msgb1 := []byte{0x0, 0x0, 0x0, 0x22, 0x0, 0x0, 0x0, 0xf1, 0x0, 0x0, 0x0, 0x1, 0x0, 0x6, 0x66, 0x72, 0x75, 0x69, 0x74, 0x73, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x5d, 0x0, 0x3, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff}
	resp1, err := ReadProduceResp(bytes.NewBuffer(msgb1), KafkaV0)
	if err != nil {
		t.Fatalf("could not read metadata response: %s", err)
	}
	expected1 := &ProduceResp{
		CorrelationID: 241,
		Topics: []ProduceRespTopic{
			{
				Name: "fruits",
				Partitions: []ProduceRespPartition{
					{
						ID:     93,
						Err:    ErrUnknownTopicOrPartition,
						Offset: -1,
					},
				},
			},
		},
	}
	if !reflect.DeepEqual(resp1, expected1) {
		t.Fatalf("expected different message: %#v", resp1)
	}

	if b, err := resp1.Bytes(); err != nil {
		t.Fatalf("cannot serialize response: %s", err)
	} else {
		if !bytes.Equal(b, msgb1) {
			t.Fatalf("serialized representation different from expected: %#v", b)
		}
	}

	msgb2 := []byte{0x0, 0x0, 0x0, 0x1f, 0x0, 0x0, 0x0, 0xf1, 0x0, 0x0, 0x0, 0x1, 0x0, 0x3, 0x66, 0x6f, 0x6f, 0x0, 0x0, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1}
	resp2, err := ReadProduceResp(bytes.NewBuffer(msgb2), KafkaV0)
	if err != nil {
		t.Fatalf("could not read metadata response: %s", err)
	}
	expected2 := &ProduceResp{
		CorrelationID: 241,
		Topics: []ProduceRespTopic{
			{
				Name: "foo",
				Partitions: []ProduceRespPartition{
					{
						ID:     0,
						Err:    error(nil),
						Offset: 1,
					},
				},
			},
		},
	}
	if !reflect.DeepEqual(resp2, expected2) {
		t.Fatalf("expected different message: %#v", resp2)
	}
	if b, err := resp2.Bytes(); err != nil {
		t.Fatalf("cannot serialize response: %s", err)
	} else {
		if !bytes.Equal(b, msgb2) {
			t.Fatalf("serialized representation different from expected: %#v", b)
		}
	}
}

func TestProduceResponseWithVersions(t *testing.T) {
	produceRespV1 := ProduceResp{
		Version:       1,
		CorrelationID: 0,
		Topics: []ProduceRespTopic{
			ProduceRespTopic{
				Name: "",
				Partitions: []ProduceRespPartition{
					ProduceRespPartition{
						ID:            0,
						Err:           nil,
						Offset:        0,
						LogAppendTime: 0,
					},
				},
			},
		},
		ThrottleTime: time.Second,
	}

	b, err := produceRespV1.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	resp, err := ReadProduceResp(bytes.NewBuffer(b), produceRespV1.Version)
	if err != nil {
		t.Fatal(err)
	}
	//assert.Equal(t, produceRespV1, *resp, "Not equal")
	if !reflect.DeepEqual(produceRespV1, *resp) {
		t.Errorf("Not equal")
	}
	produceRespV2 := produceRespV1
	produceRespV2.Version = KafkaV2
	produceRespV2.Topics[0].Partitions[0].LogAppendTime = 5

	b, err = produceRespV2.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	resp, err = ReadProduceResp(bytes.NewBuffer(b), produceRespV2.Version)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(produceRespV2, *resp) {
		t.Errorf("Not equal")
	}

}

func TestFetchRequest(t *testing.T) {
	req := &FetchReq{
		CorrelationID: 241,
		ClientID:      "test",
		ReplicaID:     -1,
		MaxWaitTime:   time.Second * 2,
		MinBytes:      12454,
		Topics: []FetchReqTopic{
			{
				Name: "foo",
				Partitions: []FetchReqPartition{
					{ID: 421, FetchOffset: 529, MaxBytes: 4921},
					{ID: 0, FetchOffset: 11, MaxBytes: 92},
				},
			},
		},
	}
	testRequestSerialization(t, req)
	b, _ := req.Bytes()
	expected := []byte{0x0, 0x0, 0x0, 0x47, 0x0, 0x1, 0x0, 0x0, 0x0, 0x0, 0x0, 0xf1, 0x0, 0x4, 0x74, 0x65, 0x73, 0x74, 0xff, 0xff, 0xff, 0xff, 0x0, 0x0, 0x7, 0xd0, 0x0, 0x0, 0x30, 0xa6, 0x0, 0x0, 0x0, 0x1, 0x0, 0x3, 0x66, 0x6f, 0x6f, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x1, 0xa5, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x11, 0x0, 0x0, 0x13, 0x39, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0xb, 0x0, 0x0, 0x0, 0x5c}

	if !bytes.Equal(b, expected) {
		t.Fatalf("expected different bytes representation: %#v", b)
	}

	r, _ := ReadFetchReq(bytes.NewBuffer(expected))
	if !reflect.DeepEqual(r, req) {
		t.Fatalf("malformed request: %#v", r)
	}
}

func TestFetchResponse(t *testing.T) {
	expected1 := &FetchResp{
		CorrelationID: 241,
		Topics: []FetchRespTopic{
			{
				Name: "foo",
				Partitions: []FetchRespPartition{
					{
						ID:        0,
						Err:       error(nil),
						TipOffset: 4,
						Messages: []*Message{
							{Offset: 2, Crc: 0xb8ba5f57, Key: []byte("foo"), Value: []byte("bar"), Topic: "foo", Partition: 0, TipOffset: 4},
							{Offset: 3, Crc: 0xb8ba5f57, Key: []byte("foo"), Value: []byte("bar"), Topic: "foo", Partition: 0, TipOffset: 4},
						},
					},
					{
						ID:        1,
						Err:       ErrUnknownTopicOrPartition,
						TipOffset: -1,
						Messages:  []*Message{},
					},
				},
			},
		},
	}

	tests := []struct {
		Bytes     []byte
		RoundTrip bool // whether to compare re-serialized version
		Expected  *FetchResp
	}{
		{ // CompressionNone
			Bytes:     []byte{0x0, 0x0, 0x0, 0x75, 0x0, 0x0, 0x0, 0xf1, 0x0, 0x0, 0x0, 0x1, 0x0, 0x3, 0x66, 0x6f, 0x6f, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x4, 0x0, 0x0, 0x0, 0x40, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x14, 0xb8, 0xba, 0x5f, 0x57, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3, 0x66, 0x6f, 0x6f, 0x0, 0x0, 0x0, 0x3, 0x62, 0x61, 0x72, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0x0, 0x14, 0xb8, 0xba, 0x5f, 0x57, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3, 0x66, 0x6f, 0x6f, 0x0, 0x0, 0x0, 0x3, 0x62, 0x61, 0x72, 0x0, 0x0, 0x0, 0x1, 0x0, 0x3, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x0, 0x0, 0x0, 0x0},
			RoundTrip: true,
			Expected:  expected1,
		},
		{ // CompressionGzip
			Bytes:     []byte{0x0, 0x0, 0x0, 0x81, 0x0, 0x0, 0x0, 0xf1, 0x0, 0x0, 0x0, 0x1, 0x0, 0x3, 0x66, 0x6f, 0x6f, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x4, 0x0, 0x0, 0x0, 0x4c, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0x0, 0x40, 0x7, 0x3c, 0x17, 0x35, 0x0, 0x1, 0xff, 0xff, 0xff, 0xff, 0x0, 0x0, 0x0, 0x32, 0x1f, 0x8b, 0x8, 0x0, 0x0, 0x9, 0x6e, 0x88, 0x0, 0xff, 0x62, 0x80, 0x0, 0x26, 0x20, 0x16, 0xd9, 0xb1, 0x2b, 0x3e, 0x1c, 0xcc, 0x63, 0x4e, 0xcb, 0xcf, 0x7, 0x51, 0x49, 0x89, 0x45, 0x50, 0x79, 0x66, 0x5c, 0xf2, 0x80, 0x0, 0x0, 0x0, 0xff, 0xff, 0xab, 0xcc, 0x83, 0x80, 0x40, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x3, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x0, 0x0, 0x0, 0x0},
			RoundTrip: false,
			Expected:  expected1,
		},
		{ // CompressionSnappy
			Bytes:     []byte{0x0, 0x0, 0x0, 0x75, 0x0, 0x0, 0x0, 0xf1, 0x0, 0x0, 0x0, 0x1, 0x0, 0x3, 0x66, 0x6f, 0x6f, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x4, 0x0, 0x0, 0x0, 0x40, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0x0, 0x34, 0x6, 0x8d, 0xfe, 0xe2, 0x0, 0x2, 0xff, 0xff, 0xff, 0xff, 0x0, 0x0, 0x0, 0x26, 0x40, 0x0, 0x0, 0x9, 0x1, 0x20, 0x2, 0x0, 0x0, 0x0, 0x14, 0xb8, 0xba, 0x5f, 0x57, 0x5, 0xf, 0x28, 0x3, 0x66, 0x6f, 0x6f, 0x0, 0x0, 0x0, 0x3, 0x62, 0x61, 0x72, 0x5, 0x10, 0x8, 0x0, 0x0, 0x3, 0x5e, 0x20, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x3, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x0, 0x0, 0x0, 0x0},
			RoundTrip: false,
			Expected:  expected1,
		},
		{
			Bytes:     []byte{0x0, 0x0, 0x0, 0x48, 0x0, 0x0, 0x0, 0xf1, 0x0, 0x0, 0x0, 0x1, 0x0, 0x4, 0x74, 0x65, 0x73, 0x74, 0x0, 0x0, 0x0, 0x3, 0x0, 0x0, 0x0, 0x0, 0x0, 0x3, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x1, 0x0, 0x3, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x8, 0x0, 0x3, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x0, 0x0, 0x0, 0x0},
			RoundTrip: true,
			Expected: &FetchResp{
				CorrelationID: 241,
				Topics: []FetchRespTopic{
					{
						Name: "test",
						Partitions: []FetchRespPartition{
							{
								ID:        0,
								Err:       ErrUnknownTopicOrPartition,
								TipOffset: -1,
								Messages:  []*Message{},
							},
							{
								ID:        1,
								Err:       ErrUnknownTopicOrPartition,
								TipOffset: -1,
								Messages:  []*Message{},
							},
							{
								ID:        8,
								Err:       ErrUnknownTopicOrPartition,
								TipOffset: -1,
								Messages:  []*Message{},
							},
						},
					},
				},
			},
		},
	}

	for _, tt := range tests {
		resp, err := ReadFetchResp(bytes.NewBuffer(tt.Bytes), KafkaV0)
		if err != nil {
			t.Fatalf("could not read fetch response: %s", err)
		}
		if !reflect.DeepEqual(resp, tt.Expected) {
			t.Fatalf("expected different message: %#v", resp)
		}
		if tt.RoundTrip {
			b, err := resp.Bytes()
			if err != nil {
				t.Fatalf("cannot serialize response: %s", err)
			}
			if !bytes.Equal(b, tt.Bytes) {
				t.Fatalf("serialized representation different from expected: %#v", b)
			}
		}
	}
}

func TestOffsetFetchWithVersions(t *testing.T) {
	respV0 := OffsetFetchResp{
		Version:       0,
		CorrelationID: 0,
		ThrottleTime:  0,
		Topics: []OffsetFetchRespTopic{
			OffsetFetchRespTopic{
				Name: "",
				Partitions: []OffsetFetchRespPartition{
					OffsetFetchRespPartition{
						ID:       0,
						Offset:   0,
						Metadata: "",
						Err:      nil,
					},
				},
			},
		},
		Err: nil,
	}

	b0, err := respV0.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	r0, err := ReadOffsetFetchResp(bytes.NewReader(b0), respV0.Version)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(respV0, *r0) {
		t.Errorf("Expected \n %#+v\n fot \n %#+v\n", respV0, *r0)
	}

	respV2 := respV0
	respV2.Version = KafkaV2
	respV2.Err = errnoToErr[-1]

	b2, err := respV2.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	r2, err := ReadOffsetFetchResp(bytes.NewReader(b2), respV2.Version)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(respV2, *r2) {
		t.Errorf("Expected \n %#+v\n fot \n %#+v\n", respV2, *r2)
	}

	respV3 := respV2
	respV3.Version = KafkaV3
	respV3.ThrottleTime = 10 * time.Second

	b3, err := respV3.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	r3, err := ReadOffsetFetchResp(bytes.NewReader(b3), respV3.Version)
	if err != nil {
		t.Fatal(err)
	}

	if !reflect.DeepEqual(respV3, *r3) {
		t.Errorf("Expected \n %#+v\n fot \n %#+v\n", respV3, *r3)
	}
}

func TestFetchResponseWithVersions(t *testing.T) {

	// Test version 0

	fetchRespV0 := FetchResp{
		Version:       KafkaV0,
		CorrelationID: 1,
		Topics: []FetchRespTopic{
			FetchRespTopic{
				Name: "Topic1",
				Partitions: []FetchRespPartition{
					FetchRespPartition{
						ID:        1,
						Err:       nil,
						TipOffset: 1,
						Messages:  []*Message{},
					},
				},
			},
		},
	}

	b0, err := fetchRespV0.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	resp0, err := ReadFetchResp(bytes.NewReader(b0), fetchRespV0.Version)
	if !reflect.DeepEqual(&fetchRespV0, resp0) {
		t.Fatalf("Not equal %+#v ,  %+#v", fetchRespV0, resp0)
	}

	// Test version 1

	fetchRespV1 := fetchRespV0
	fetchRespV1.Version = KafkaV1
	fetchRespV1.ThrottleTime = time.Second

	b1, err := fetchRespV1.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	resp1, err := ReadFetchResp(bytes.NewBuffer(b1), fetchRespV1.Version)
	if !reflect.DeepEqual(&fetchRespV1, resp1) {
		t.Fatalf("Not equal %+#v ,  %+#v", fetchRespV1, resp1)
	}

	// Test version 4

	fetchRespV4 := fetchRespV1
	fetchRespV4.Version = KafkaV4
	fetchRespV4.Topics[0].Partitions[0].LastStableOffset = 1
	fetchRespV4.Topics[0].Partitions[0].AbortedTransactions = []FetchRespAbortedTransaction{
		FetchRespAbortedTransaction{
			ProducerID:  1,
			FirstOffset: 1,
		},
	}

	b4, err := fetchRespV4.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	resp4, err := ReadFetchResp(bytes.NewBuffer(b4), fetchRespV4.Version)
	if !reflect.DeepEqual(&fetchRespV4, resp4) {
		t.Fatalf("Not equal %+#v ,  %+#v", fetchRespV4, resp4)
	}

	// Test version 5

	fetchRespV5 := fetchRespV4
	fetchRespV5.Version = KafkaV5

	fetchRespV5.Topics[0].Partitions[0].LogStartOffset = 1

	b5, err := fetchRespV5.Bytes()
	if err != nil {
		t.Fatal(err)
	}

	resp5, err := ReadFetchResp(bytes.NewBuffer(b5), fetchRespV5.Version)
	if !reflect.DeepEqual(&fetchRespV5, resp5) {
		t.Fatalf("Not equal %+#v ,  %+#v", fetchRespV5, resp5)
	}

}

func TestOffsetCommitResponseWithVersions(t *testing.T) {
	respV0 := OffsetCommitResp{
		Version:       KafkaV0,
		CorrelationID: 1,
		ThrottleTime:  0,
		Topics: []OffsetCommitRespTopic{
			OffsetCommitRespTopic{
				Name: "test",
				Partitions: []OffsetCommitRespPartition{
					OffsetCommitRespPartition{
						ID:  1,
						Err: nil,
					},
				},
			},
		},
	}

	b0, err := respV0.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	resp0, err := ReadOffsetCommitResp(bytes.NewReader(b0), respV0.Version)

	if !reflect.DeepEqual(&respV0, resp0) {
		t.Fatalf("Not equal %+#v ,  %+#v", respV0, resp0)
	}

	respV3 := respV0
	respV3.Version = KafkaV3
	respV3.ThrottleTime = 2 * time.Second

	b3, err := respV3.Bytes()
	if err != nil {
		t.Fatal(err)
	}
	resp3, err := ReadOffsetCommitResp(bytes.NewReader(b3), respV3.Version)

	if !reflect.DeepEqual(respV3, *resp3) {
		t.Fatalf("Not equal \n%+#v ,  \n%+#v", respV3, *resp3)
	}

}

func TestSerializeEmptyMessageSet(t *testing.T) {
	var buf bytes.Buffer
	messages := []*Message{}
	n, err := writeMessageSet(&buf, messages, CompressionNone)
	if err != nil {
		t.Fatalf("cannot serialize messages: %s", err)
	}
	if n != 0 {
		t.Fatalf("got n=%d result from writeMessageSet; want 0", n)
	}
	if l := len(buf.Bytes()); l != 0 {
		t.Fatalf("got len=%d for empty message set; should be 0", l)
	}
}

func TestReadIncompleteMessage(t *testing.T) {
	var buf bytes.Buffer
	_, err := writeMessageSet(&buf, []*Message{
		{Value: []byte("111111111111111")},
		{Value: []byte("222222222222222")},
		{Value: []byte("333333333333333")},
	}, CompressionNone)
	if err != nil {
		t.Fatalf("cannot serialize messages: %s", err)
	}

	b := buf.Bytes()
	// cut off the last bytes as kafka can do
	b = b[:len(b)-4]
	messages, err := readMessageSet(bytes.NewBuffer(b), int32(len(b)), 0)
	if err != nil {
		t.Fatalf("cannot deserialize messages: %s", err)
	}
	if len(messages) != 2 {
		t.Fatalf("expected 2 messages, got %d", len(messages))
	}
	if messages[0].Value[0] != '1' || messages[1].Value[0] != '2' {
		t.Fatal("expected different messages content")
	}
}

func TestReadEmptyMessage(t *testing.T) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	message := Message{}
	enc.EncodeInt64(message.Offset)
	enc.EncodeInt32(0)
	if err := enc.Err(); err != nil {
		t.Fatalf("encoding error: %s", err)
	}

	b := buf.Bytes()
	messages, err := readMessageSet(bytes.NewBuffer(b), int32(len(b)), 0)
	if err != nil {
		t.Fatalf("cannot deserialize messages: %s", err)
	}
	if len(messages) != 0 {
		t.Fatalf("expected 0 messages, got %d", len(messages))
	}
}

func BenchmarkProduceRequestMarshal(b *testing.B) {
	messages := make([]*Message, 100)
	for i := range messages {
		messages[i] = &Message{
			Offset: int64(i),
			Crc:    uint32(i),
			Key:    nil,
			Value:  []byte(`Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec a diam lectus. Sed sit amet ipsum mauris. Maecenas congue ligula ac quam viverra nec consectetur ante hendrerit. Donec et mollis dolor. Praesent et diam eget libero egestas mattis sit amet vitae augue. Nam tincidunt congue enim, ut porta lorem lacinia consectetur.`),
		}

	}
	req := &ProduceReq{
		CorrelationID: 241,
		ClientID:      "test",
		Compression:   CompressionNone,
		RequiredAcks:  RequiredAcksAll,
		Timeout:       time.Second,
		Topics: []ProduceReqTopic{
			{
				Name: "foo",
				Partitions: []ProduceReqPartition{
					{
						ID:       0,
						Messages: messages,
					},
				},
			},
		},
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := req.Bytes(); err != nil {
			b.Fatalf("could not serialize messages: %s", err)
		}
	}
}

func BenchmarkProduceResponseUnmarshal(b *testing.B) {
	resp := &ProduceResp{
		CorrelationID: 241,
		Topics: []ProduceRespTopic{
			{
				Name: "foo",
				Partitions: []ProduceRespPartition{
					{
						ID:     0,
						Err:    error(nil),
						Offset: 1,
					},
				},
			},
		},
	}
	raw, err := resp.Bytes()
	if err != nil {
		b.Fatalf("cannot serialize response: %s", err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := ReadProduceResp(bytes.NewBuffer(raw), resp.Version); err != nil {
			b.Fatalf("could not deserialize messages: %s", err)
		}
	}
}

func BenchmarkFetchRequestMarshal(b *testing.B) {
	req := &FetchReq{
		CorrelationID: 241,
		ClientID:      "test",
		MaxWaitTime:   time.Second * 2,
		MinBytes:      12454,
		Topics: []FetchReqTopic{
			{
				Name: "foo",
				Partitions: []FetchReqPartition{
					{ID: 421, FetchOffset: 529, MaxBytes: 4921},
					{ID: 0, FetchOffset: 11, MaxBytes: 92},
				},
			},
		},
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := req.Bytes(); err != nil {
			b.Fatalf("could not serialize messages: %s", err)
		}
	}
}

func BenchmarkFetchResponseUnmarshal(b *testing.B) {
	messages := make([]*Message, 100)
	for i := range messages {
		messages[i] = &Message{
			Offset: int64(i),
			Key:    nil,
			Value:  []byte(`Lorem ipsum dolor sit amet, consectetur adipiscing elit. Donec a diam lectus. Sed sit amet ipsum mauris. Maecenas congue ligula ac quam viverra nec consectetur ante hendrerit. Donec et mollis dolor. Praesent et diam eget libero egestas mattis sit amet vitae augue. Nam tincidunt congue enim, ut porta lorem lacinia consectetur.`),
		}

	}
	resp := &FetchResp{
		CorrelationID: 241,
		Topics: []FetchRespTopic{
			{
				Name: "foo",
				Partitions: []FetchRespPartition{
					{
						ID:        0,
						TipOffset: 444,
						Messages:  messages,
					},
					{
						ID:        123,
						Err:       ErrBrokerNotAvailable,
						TipOffset: -1,
						Messages:  []*Message{},
					},
				},
			},
		},
	}
	raw, err := resp.Bytes()
	if err != nil {
		b.Fatalf("cannot serialize response: %s", err)
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		if _, err := ReadFetchResp(bytes.NewBuffer(raw), KafkaV0); err != nil {
			b.Fatalf("could not deserialize messages: %s", err)
		}
	}
}
