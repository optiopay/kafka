package proto

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"time"
)

/*

Kafka wire protocol implemented as described in
https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Messagesets

*/

const (
	produceReq          = 0
	fetchReq            = 1
	offsetReq           = 2
	metadataReq         = 3
	offsetCommitReq     = 8
	offsetFetchReq      = 9
	consumerMetadataReq = 10

	compressionNone   = 0
	compressionGZIP   = 1
	compressionSnappy = 2

	// receive the latest offset (i.e. the offset of the next coming message)
	OffsetReqTimeLatest = -1

	// receive the earliest available offset. Note that because offsets are
	// pulled in descending order, asking for the earliest offset will always
	// return you a single element.
	OffsetReqTimeEarliest = -2

	// Server will not send any response.
	RequiredAcksNone = 0

	// Server will block until the message is committed by all in sync replicas
	// before sending a response.
	RequiredAcksAll = -1

	// Server will wait the data is written to the local log before sending a
	// response.
	RequiredAcksLocal = 1
)

// ReadReq returns request kind ID and byte representation of the whole message
// in wire protocol format.
func ReadReq(r io.Reader) (requestKind int16, b []byte, err error) {
	dec := NewDecoder(r)
	msgSize := dec.DecodeInt32()
	requestKind = dec.DecodeInt16()
	if err := dec.Err(); err != nil {
		return 0, nil, err
	}
	// size of the message + size of the message itself
	b = make([]byte, msgSize+4)
	binary.BigEndian.PutUint32(b, uint32(msgSize))
	binary.BigEndian.PutUint16(b[4:], uint16(requestKind))
	if _, err := io.ReadFull(r, b[6:]); err != nil {
		return 0, nil, err
	}
	return requestKind, b, err
}

// ReadResp returns message correlation ID and byte representation of the whole
// message in wire protocol that is returned when reading from given stream,
// including 4 bytes of message size itself.
// Byte representation returned by ReadResp can be parsed by all response
// reeaders to transform it into specialized response structure.
func ReadResp(r io.Reader) (correlationID int32, b []byte, err error) {
	dec := NewDecoder(r)
	msgSize := dec.DecodeInt32()
	correlationID = dec.DecodeInt32()
	if err := dec.Err(); err != nil {
		return 0, nil, err
	}
	// size of the message + size of the message itself
	b = make([]byte, msgSize+4)
	binary.BigEndian.PutUint32(b, uint32(msgSize))
	binary.BigEndian.PutUint32(b[4:], uint32(correlationID))
	_, err = io.ReadFull(r, b[8:])
	return correlationID, b, err
}

// Message represents single entity of message set.
type Message struct {
	Key       []byte
	Value     []byte
	Offset    int64  // set when fetching and after successful producing
	Crc       uint32 // set when fetching, ignored when producing
	Topic     string // set when fetching, ignored when producing
	Partition int32  // set when fetching, ignored when producing
}

// ComputeCrc returns crc32 hash for given message content.
func ComputeCrc(m *Message) uint32 {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)
	enc.Encode(int8(0)) // magic byte is always 0
	enc.Encode(int8(0)) // no compression support
	enc.Encode(m.Key)
	enc.Encode(m.Value)
	return crc32.ChecksumIEEE(buf.Bytes())
}

// writeMessageSet writes given set of messages into writer, prefixed with
// total set size.
func writeMessageSet(w io.Writer, messages []*Message) error {
	enc := NewEncoder(w)

	totalSize := 0
	for _, m := range messages {
		totalSize += 26 + len(m.Key) + len(m.Value)
	}
	enc.Encode(int32(totalSize))

	for _, message := range messages {
		enc.Encode(int64(message.Offset))
		messageSize := 14 + len(message.Key) + len(message.Value)
		enc.Encode(int32(messageSize))
		enc.Encode(ComputeCrc(message))
		enc.Encode(int8(0)) // magic byte
		enc.Encode(int8(0)) // attributes
		enc.Encode(message.Key)
		enc.Encode(message.Value)
	}
	return enc.Err()
}

// readMessageSet reads and return messages from the stream. Messages set
// should be prefixed with message set size, that will also be consumed by this
// function.
// Because kafka is sending message set directly from the drive, it might cut
// off part of the last message. This also means that the last message can be
// shorter than the header is saying. In such case just ignore the last
// malformed message from the set and returned earlier data.
func readMessageSet(r io.Reader) ([]*Message, error) {
	dec := NewDecoder(r)
	messagesSetSize := dec.DecodeInt32()
	if err := dec.Err(); err != nil {
		return nil, err
	}

	rd := io.LimitReader(r, int64(messagesSetSize))
	dec = NewDecoder(rd)
	set := make([]*Message, 0, 32)
	for {
		offset := dec.DecodeInt64()
		if err := dec.Err(); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return set, nil
			}
			return nil, err
		}
		// single message size
		size := dec.DecodeInt32()
		if err := dec.Err(); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return set, nil
			}
			return nil, err
		}

		// read message to buffer to compute it's content crc
		msgbuf := make([]byte, size)
		if _, err := io.ReadFull(rd, msgbuf); err != nil {
			if err == io.EOF || err == io.ErrUnexpectedEOF {
				return set, nil
			}
			return nil, err
		}
		msgdec := NewDecoder(bytes.NewBuffer(msgbuf))

		msg := &Message{Offset: offset}
		msg.Crc = msgdec.DecodeUint32()

		if msg.Crc != crc32.ChecksumIEEE(msgbuf[4:]) {
			// ignore this message and because we want to have constant
			// history, do not process anything more
			return set, nil
		}

		// magic byte
		_ = msgdec.DecodeInt8()

		attributes := msgdec.DecodeInt8()
		if attributes != compressionNone {
			// TODO(husio)
			return nil, errors.New("cannot read compressed message")
		}

		msg.Key = msgdec.DecodeBytes()
		msg.Value = msgdec.DecodeBytes()

		if err := msgdec.Err(); err != nil {
			return nil, fmt.Errorf("cannot decode message: %s", err)
		}
		set = append(set, msg)
	}
}

type MetadataReq struct {
	CorrelationID int32
	ClientID      string
	Topics        []string
}

func ReadMetadataReq(r io.Reader) (*MetadataReq, error) {
	var req MetadataReq
	dec := NewDecoder(r)

	// total message size
	_ = dec.DecodeInt32()
	// api key + api version
	_ = dec.DecodeInt32()
	req.CorrelationID = dec.DecodeInt32()
	req.ClientID = dec.DecodeString()
	req.Topics = make([]string, dec.DecodeArrayLen())
	for i := range req.Topics {
		req.Topics[i] = dec.DecodeString()
	}

	if dec.Err() != nil {
		return nil, dec.Err()
	}
	return &req, nil
}

func (r *MetadataReq) Bytes() ([]byte, error) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)

	// message size - for now just placeholder
	enc.Encode(int32(0))
	enc.Encode(int16(metadataReq))
	enc.Encode(int16(0))
	enc.Encode(r.CorrelationID)
	enc.Encode(r.ClientID)

	enc.EncodeArrayLen(len(r.Topics))
	for _, name := range r.Topics {
		enc.Encode(name)
	}

	if enc.Err() != nil {
		return nil, enc.Err()
	}

	// update the message size information
	b := buf.Bytes()
	binary.BigEndian.PutUint32(b, uint32(len(b)-4))

	return b, nil
}

func (r *MetadataReq) WriteTo(w io.Writer) (int64, error) {
	b, err := r.Bytes()
	if err != nil {
		return 0, err
	}
	n, err := w.Write(b)
	return int64(n), err
}

type MetadataResp struct {
	CorrelationID int32
	Brokers       []MetadataRespBroker
	Topics        []MetadataRespTopic
}

type MetadataRespBroker struct {
	NodeID int32
	Host   string
	Port   int32
}

type MetadataRespTopic struct {
	Name       string
	Err        error
	Partitions []MetadataRespPartition
}

type MetadataRespPartition struct {
	ID       int32
	Err      error
	Leader   int32
	Replicas []int32
	Isrs     []int32
}

func (r *MetadataResp) Bytes() ([]byte, error) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)

	// message size - for now just placeholder
	enc.Encode(int32(0))
	enc.Encode(r.CorrelationID)
	enc.EncodeArrayLen(len(r.Brokers))
	for _, broker := range r.Brokers {
		enc.Encode(broker.NodeID)
		enc.Encode(broker.Host)
		enc.Encode(broker.Port)
	}
	enc.EncodeArrayLen(len(r.Topics))
	for _, topic := range r.Topics {
		enc.EncodeError(topic.Err)
		enc.Encode(topic.Name)
		enc.EncodeArrayLen(len(topic.Partitions))
		for _, part := range topic.Partitions {
			enc.EncodeError(part.Err)
			enc.Encode(part.ID)
			enc.Encode(part.Leader)
			enc.Encode(part.Replicas)
			enc.Encode(part.Isrs)
		}
	}

	if enc.Err() != nil {
		return nil, enc.Err()
	}

	// update the message size information
	b := buf.Bytes()
	binary.BigEndian.PutUint32(b, uint32(len(b)-4))

	return b, nil
}

func ReadMetadataResp(r io.Reader) (*MetadataResp, error) {
	var resp MetadataResp
	dec := NewDecoder(r)

	// total message size
	_ = dec.DecodeInt32()
	resp.CorrelationID = dec.DecodeInt32()

	resp.Brokers = make([]MetadataRespBroker, dec.DecodeArrayLen())
	for i := range resp.Brokers {
		var b = &resp.Brokers[i]
		b.NodeID = dec.DecodeInt32()
		b.Host = dec.DecodeString()
		b.Port = dec.DecodeInt32()
	}

	resp.Topics = make([]MetadataRespTopic, dec.DecodeArrayLen())
	for ti := range resp.Topics {
		var t = &resp.Topics[ti]
		t.Err = errFromNo(dec.DecodeInt16())
		t.Name = dec.DecodeString()
		t.Partitions = make([]MetadataRespPartition, dec.DecodeArrayLen())
		for pi := range t.Partitions {
			var p = &t.Partitions[pi]
			p.Err = errFromNo(dec.DecodeInt16())
			p.ID = dec.DecodeInt32()
			p.Leader = dec.DecodeInt32()

			p.Replicas = make([]int32, dec.DecodeArrayLen())
			for ri := range p.Replicas {
				p.Replicas[ri] = dec.DecodeInt32()
			}

			p.Isrs = make([]int32, dec.DecodeArrayLen())
			for ii := range p.Isrs {
				p.Isrs[ii] = dec.DecodeInt32()
			}
		}
	}

	if dec.Err() != nil {
		return nil, dec.Err()
	}
	return &resp, nil
}

type FetchReq struct {
	CorrelationID int32
	ClientID      string
	MaxWaitTime   time.Duration
	MinBytes      int32

	Topics []FetchReqTopic
}

type FetchReqTopic struct {
	Name       string
	Partitions []FetchReqPartition
}

type FetchReqPartition struct {
	ID          int32
	FetchOffset int64
	MaxBytes    int32
}

func ReadFetchReq(r io.Reader) (*FetchReq, error) {
	var req FetchReq
	dec := NewDecoder(r)

	// total message size
	_ = dec.DecodeInt32()
	// api key + api version
	_ = dec.DecodeInt32()
	req.CorrelationID = dec.DecodeInt32()
	req.ClientID = dec.DecodeString()
	// replica id
	_ = dec.DecodeInt32()
	req.MaxWaitTime = time.Duration(dec.DecodeInt32()) * time.Millisecond
	req.MinBytes = dec.DecodeInt32()
	req.Topics = make([]FetchReqTopic, dec.DecodeArrayLen())
	for ti := range req.Topics {
		var topic = &req.Topics[ti]
		topic.Name = dec.DecodeString()
		topic.Partitions = make([]FetchReqPartition, dec.DecodeArrayLen())
		for pi := range topic.Partitions {
			var part = &topic.Partitions[pi]
			part.ID = dec.DecodeInt32()
			part.FetchOffset = dec.DecodeInt64()
			part.MaxBytes = dec.DecodeInt32()
		}
	}

	if dec.Err() != nil {
		return nil, dec.Err()
	}
	return &req, nil
}

func (r *FetchReq) Bytes() ([]byte, error) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)

	// message size - for now just placeholder
	enc.Encode(int32(0))
	enc.Encode(int16(fetchReq))
	enc.Encode(int16(0))
	enc.Encode(r.CorrelationID)
	enc.Encode(r.ClientID)

	// replica id
	enc.Encode(int32(-1))
	enc.Encode(int32(r.MaxWaitTime / time.Millisecond))
	enc.Encode(r.MinBytes)

	enc.EncodeArrayLen(len(r.Topics))
	for _, topic := range r.Topics {
		enc.Encode(topic.Name)
		enc.EncodeArrayLen(len(topic.Partitions))
		for _, part := range topic.Partitions {
			enc.Encode(part.ID)
			enc.Encode(part.FetchOffset)
			enc.Encode(part.MaxBytes)
		}
	}

	if enc.Err() != nil {
		return nil, enc.Err()
	}

	// update the message size information
	b := buf.Bytes()
	binary.BigEndian.PutUint32(b, uint32(len(b)-4))

	return b, nil
}

func (r *FetchReq) WriteTo(w io.Writer) (int64, error) {
	b, err := r.Bytes()
	if err != nil {
		return 0, err
	}
	n, err := w.Write(b)
	return int64(n), err
}

type FetchResp struct {
	CorrelationID int32
	Topics        []FetchRespTopic
}

type FetchRespTopic struct {
	Name       string
	Partitions []FetchRespPartition
}

type FetchRespPartition struct {
	ID        int32
	Err       error
	TipOffset int64
	Messages  []*Message
}

func (r *FetchResp) Bytes() ([]byte, error) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)

	// message size - for now just placeholder
	enc.Encode(int32(0))
	enc.Encode(r.CorrelationID)
	enc.EncodeArrayLen(len(r.Topics))
	for _, topic := range r.Topics {
		enc.Encode(topic.Name)
		enc.EncodeArrayLen(len(topic.Partitions))
		for _, part := range topic.Partitions {
			enc.Encode(part.ID)
			enc.EncodeError(part.Err)
			enc.Encode(part.TipOffset)
			if err := writeMessageSet(&buf, part.Messages); err != nil {
				return nil, err
			}
		}
	}

	if enc.Err() != nil {
		return nil, enc.Err()
	}

	// update the message size information
	b := buf.Bytes()
	binary.BigEndian.PutUint32(b, uint32(len(b)-4))

	return b, nil
}

func ReadFetchResp(r io.Reader) (*FetchResp, error) {
	var err error
	var resp FetchResp

	dec := NewDecoder(r)

	// total message size
	_ = dec.DecodeInt32()
	resp.CorrelationID = dec.DecodeInt32()

	resp.Topics = make([]FetchRespTopic, dec.DecodeArrayLen())
	for ti := range resp.Topics {
		var topic = &resp.Topics[ti]
		topic.Name = dec.DecodeString()
		topic.Partitions = make([]FetchRespPartition, dec.DecodeArrayLen())
		for pi := range topic.Partitions {
			var part = &topic.Partitions[pi]
			part.ID = dec.DecodeInt32()
			part.Err = errFromNo(dec.DecodeInt16())
			part.TipOffset = dec.DecodeInt64()
			if dec.Err() != nil {
				return nil, dec.Err()
			}
			if part.Messages, err = readMessageSet(r); err != nil {
				return nil, err
			}
			for _, msg := range part.Messages {
				msg.Topic = topic.Name
				msg.Partition = part.ID
			}
		}
	}

	if dec.Err() != nil {
		return nil, dec.Err()
	}
	return &resp, nil
}

type ConsumerMetadataReq struct {
	CorrelationID int32
	ClientID      string
	ConsumerGroup string
}

func ReadConsumerMetadataReq(r io.Reader) (*ConsumerMetadataReq, error) {
	var req ConsumerMetadataReq
	dec := NewDecoder(r)

	// total message size
	_ = dec.DecodeInt32()
	// api key + api version
	_ = dec.DecodeInt32()
	req.CorrelationID = dec.DecodeInt32()
	req.ClientID = dec.DecodeString()
	req.ConsumerGroup = dec.DecodeString()

	if dec.Err() != nil {
		return nil, dec.Err()
	}
	return &req, nil
}

func (r *ConsumerMetadataReq) Bytes() ([]byte, error) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)

	// message size - for now just placeholder
	enc.Encode(int32(0))
	enc.Encode(int16(consumerMetadataReq))
	enc.Encode(int16(0))
	enc.Encode(r.CorrelationID)
	enc.Encode(r.ClientID)

	enc.Encode(r.ConsumerGroup)

	if enc.Err() != nil {
		return nil, enc.Err()
	}

	// update the message size information
	b := buf.Bytes()
	binary.BigEndian.PutUint32(b, uint32(len(b)-4))

	return b, nil
}

func (r *ConsumerMetadataReq) WriteTo(w io.Writer) (int64, error) {
	b, err := r.Bytes()
	if err != nil {
		return 0, err
	}
	n, err := w.Write(b)
	return int64(n), err
}

type ConsumerMetadataResp struct {
	CorrelationID   int32
	Err             error
	CoordinatorID   int32
	CoordinatorHost string
	CoordinatorPort int32
}

func ReadConsumerMetadataResp(r io.Reader) (*ConsumerMetadataResp, error) {
	var resp ConsumerMetadataResp
	dec := NewDecoder(r)

	// total message size
	_ = dec.DecodeInt32()
	resp.CorrelationID = dec.DecodeInt32()
	resp.Err = errFromNo(dec.DecodeInt16())
	resp.CoordinatorID = dec.DecodeInt32()
	resp.CoordinatorHost = dec.DecodeString()
	resp.CoordinatorPort = dec.DecodeInt32()

	if err := dec.Err(); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (r *ConsumerMetadataResp) Bytes() ([]byte, error) {
	return nil, errors.New("not implemented")
}

type OffsetCommitReq struct {
	CorrelationID int32
	ClientID      string
	ConsumerGroup string
	Topics        []OffsetCommitReqTopic
}

type OffsetCommitReqTopic struct {
	Name       string
	Partitions []OffsetCommitReqPartition
}

type OffsetCommitReqPartition struct {
	ID        int32
	Offset    int64
	TimeStamp time.Time
	Metadata  string
}

func ReadOffsetCommitReq(r io.Reader) (*OffsetCommitReq, error) {
	var req OffsetCommitReq
	dec := NewDecoder(r)

	// total message size
	_ = dec.DecodeInt32()
	// api key + api version
	_ = dec.DecodeInt32()
	req.CorrelationID = dec.DecodeInt32()
	req.ClientID = dec.DecodeString()
	req.ConsumerGroup = dec.DecodeString()
	req.Topics = make([]OffsetCommitReqTopic, dec.DecodeArrayLen())
	for ti := range req.Topics {
		var topic = &req.Topics[ti]
		topic.Name = dec.DecodeString()
		topic.Partitions = make([]OffsetCommitReqPartition, dec.DecodeArrayLen())
		for pi := range topic.Partitions {
			var part = &topic.Partitions[pi]
			part.ID = dec.DecodeInt32()
			part.Offset = dec.DecodeInt64()
			part.TimeStamp = time.Unix(0, dec.DecodeInt64()*int64(time.Millisecond))
			part.Metadata = dec.DecodeString()
		}
	}

	if dec.Err() != nil {
		return nil, dec.Err()
	}
	return &req, nil
}

func (r *OffsetCommitReq) Bytes() ([]byte, error) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)

	// message size - for now just placeholder
	enc.Encode(int32(0))
	enc.Encode(int16(offsetCommitReq))
	enc.Encode(int16(0))
	enc.Encode(r.CorrelationID)
	enc.Encode(r.ClientID)

	enc.Encode(r.ConsumerGroup)

	enc.EncodeArrayLen(len(r.Topics))
	for _, topic := range r.Topics {
		enc.Encode(topic.Name)
		enc.EncodeArrayLen(len(topic.Partitions))
		for _, part := range topic.Partitions {
			enc.Encode(part.ID)
			enc.Encode(part.Offset)
			// TODO(husio) is this really in milliseconds?
			enc.Encode(part.TimeStamp.UnixNano() / int64(time.Millisecond))
			enc.Encode(part.Metadata)
		}
	}

	if enc.Err() != nil {
		return nil, enc.Err()
	}

	// update the message size information
	b := buf.Bytes()
	binary.BigEndian.PutUint32(b, uint32(len(b)-4))

	return b, nil
}

func (r *OffsetCommitReq) WriteTo(w io.Writer) (int64, error) {
	b, err := r.Bytes()
	if err != nil {
		return 0, err
	}
	n, err := w.Write(b)
	return int64(n), err
}

type OffsetCommitResp struct {
	CorrelationID int32
	Topics        []OffsetCommitRespTopic
}

type OffsetCommitRespTopic struct {
	Name       string
	Partitions []OffsetCommitRespPartition
}

type OffsetCommitRespPartition struct {
	ID  int32
	Err error
}

func ReadOffsetCommitResp(r io.Reader) (*OffsetCommitResp, error) {
	var resp OffsetCommitResp
	dec := NewDecoder(r)

	// total message size
	_ = dec.DecodeInt32()
	resp.CorrelationID = dec.DecodeInt32()
	resp.Topics = make([]OffsetCommitRespTopic, dec.DecodeArrayLen())
	for ti := range resp.Topics {
		var t = &resp.Topics[ti]
		t.Name = dec.DecodeString()
		t.Partitions = make([]OffsetCommitRespPartition, dec.DecodeArrayLen())
		for pi := range t.Partitions {
			var p = &t.Partitions[pi]
			p.ID = dec.DecodeInt32()
			p.Err = errFromNo(dec.DecodeInt16())
		}
	}

	if err := dec.Err(); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (r *OffsetCommitResp) Bytes() ([]byte, error) {
	return nil, errors.New("not implemented")
}

type OffsetFetchReq struct {
	CorrelationID int32
	ClientID      string
	ConsumerGroup string
	Topics        []OffsetFetchReqTopic
}

type OffsetFetchReqTopic struct {
	Name       string
	Partitions []int32
}

func ReadOffsetFetchReq(r io.Reader) (*OffsetFetchReq, error) {
	var req OffsetFetchReq
	dec := NewDecoder(r)

	// total message size
	_ = dec.DecodeInt32()
	// api key + api version
	_ = dec.DecodeInt32()
	req.CorrelationID = dec.DecodeInt32()
	req.ClientID = dec.DecodeString()
	req.ConsumerGroup = dec.DecodeString()
	req.Topics = make([]OffsetFetchReqTopic, dec.DecodeArrayLen())
	for ti := range req.Topics {
		var topic = &req.Topics[ti]
		topic.Name = dec.DecodeString()
		topic.Partitions = make([]int32, dec.DecodeArrayLen())
		for pi := range topic.Partitions {
			topic.Partitions[pi] = dec.DecodeInt32()
		}
	}

	if dec.Err() != nil {
		return nil, dec.Err()
	}
	return &req, nil
}

func (r *OffsetFetchReq) Bytes() ([]byte, error) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)

	// message size - for now just placeholder
	enc.Encode(int32(0))
	enc.Encode(int16(offsetFetchReq))
	enc.Encode(int16(0))
	enc.Encode(r.CorrelationID)
	enc.Encode(r.ClientID)

	enc.Encode(r.ConsumerGroup)
	enc.EncodeArrayLen(len(r.Topics))
	for _, t := range r.Topics {
		enc.Encode(t.Name)
		enc.EncodeArrayLen(len(t.Partitions))
		for _, p := range t.Partitions {
			enc.Encode(p)
		}
	}

	if enc.Err() != nil {
		return nil, enc.Err()
	}

	// update the message size information
	b := buf.Bytes()
	binary.BigEndian.PutUint32(b, uint32(len(b)-4))

	return b, nil
}

func (r *OffsetFetchReq) WriteTo(w io.Writer) (int64, error) {
	b, err := r.Bytes()
	if err != nil {
		return 0, err
	}
	n, err := w.Write(b)
	return int64(n), err
}

type OffsetFetchResp struct {
	CorrelationID int32
	Topics        []OffsetFetchRespTopic
}

type OffsetFetchRespTopic struct {
	Name       string
	Partitions []OffsetFetchRespPartition
}

type OffsetFetchRespPartition struct {
	ID       int32
	Offset   int64
	Metadata string
	Err      error
}

func ReadOffsetFetchResp(r io.Reader) (*OffsetFetchResp, error) {
	var resp OffsetFetchResp
	dec := NewDecoder(r)

	// total message size
	_ = dec.DecodeInt32()
	resp.CorrelationID = dec.DecodeInt32()
	resp.Topics = make([]OffsetFetchRespTopic, dec.DecodeArrayLen())
	for ti := range resp.Topics {
		var t = &resp.Topics[ti]
		t.Name = dec.DecodeString()
		t.Partitions = make([]OffsetFetchRespPartition, dec.DecodeArrayLen())
		for pi := range t.Partitions {
			var p = &t.Partitions[pi]
			p.ID = dec.DecodeInt32()
			p.Offset = dec.DecodeInt64()
			p.Metadata = dec.DecodeString()
			p.Err = errFromNo(dec.DecodeInt16())
		}
	}

	if err := dec.Err(); err != nil {
		return nil, err
	}
	return &resp, nil
}

type ProduceReq struct {
	CorrelationID int32
	ClientID      string
	RequiredAcks  int16
	Timeout       time.Duration
	Topics        []ProduceReqTopic
}

type ProduceReqTopic struct {
	Name       string
	Partitions []ProduceReqPartition
}

type ProduceReqPartition struct {
	ID       int32
	Messages []*Message
}

func ReadProduceReq(r io.Reader) (*ProduceReq, error) {
	var req ProduceReq
	dec := NewDecoder(r)

	// total message size
	_ = dec.DecodeInt32()
	// api key + api version
	_ = dec.DecodeInt32()
	req.CorrelationID = dec.DecodeInt32()
	req.ClientID = dec.DecodeString()
	req.RequiredAcks = dec.DecodeInt16()
	req.Timeout = time.Duration(dec.DecodeInt32()) * time.Millisecond
	req.Topics = make([]ProduceReqTopic, dec.DecodeArrayLen())
	for ti := range req.Topics {
		var topic = &req.Topics[ti]
		topic.Name = dec.DecodeString()
		topic.Partitions = make([]ProduceReqPartition, dec.DecodeArrayLen())
		for pi := range topic.Partitions {
			var part = &topic.Partitions[pi]
			part.ID = dec.DecodeInt32()
			err := dec.Err()
			if err != nil {
				return nil, dec.Err()
			}
			if part.Messages, err = readMessageSet(r); err != nil {
				return nil, err
			}
		}
	}

	if dec.Err() != nil {
		return nil, dec.Err()
	}
	return &req, nil
}

func (r *ProduceReq) Bytes() ([]byte, error) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)

	// message size - for now just placeholder
	enc.Encode(int32(0))
	enc.Encode(int16(produceReq))
	enc.Encode(int16(0))
	enc.Encode(r.CorrelationID)
	enc.Encode(r.ClientID)

	enc.Encode(r.RequiredAcks)
	enc.Encode(int32(r.Timeout / time.Millisecond))
	enc.EncodeArrayLen(len(r.Topics))
	for _, t := range r.Topics {
		enc.Encode(t.Name)
		enc.EncodeArrayLen(len(t.Partitions))
		for _, p := range t.Partitions {
			enc.Encode(p.ID)
			if err := writeMessageSet(&buf, p.Messages); err != nil {
				return nil, err
			}
		}
	}

	if enc.Err() != nil {
		return nil, enc.Err()
	}

	// update the message size information
	b := buf.Bytes()
	binary.BigEndian.PutUint32(b, uint32(len(b)-4))

	return b, nil
}

func (r *ProduceReq) WriteTo(w io.Writer) (int64, error) {
	b, err := r.Bytes()
	if err != nil {
		return 0, err
	}
	n, err := w.Write(b)
	return int64(n), err
}

type ProduceResp struct {
	CorrelationID int32
	Topics        []ProduceRespTopic
}

type ProduceRespTopic struct {
	Name       string
	Partitions []ProduceRespPartition
}

type ProduceRespPartition struct {
	ID     int32
	Err    error
	Offset int64
}

func (r *ProduceResp) Bytes() ([]byte, error) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)

	// message size - for now just placeholder
	enc.Encode(int32(0))
	enc.Encode(r.CorrelationID)
	enc.EncodeArrayLen(len(r.Topics))
	for _, topic := range r.Topics {
		enc.Encode(topic.Name)
		enc.EncodeArrayLen(len(topic.Partitions))
		for _, part := range topic.Partitions {
			enc.Encode(part.ID)
			enc.EncodeError(part.Err)
			enc.Encode(part.Offset)
		}
	}

	if enc.Err() != nil {
		return nil, enc.Err()
	}

	// update the message size information
	b := buf.Bytes()
	binary.BigEndian.PutUint32(b, uint32(len(b)-4))

	return b, nil
}

func ReadProduceResp(r io.Reader) (*ProduceResp, error) {
	var resp ProduceResp
	dec := NewDecoder(r)

	// total message size
	_ = dec.DecodeInt32()
	resp.CorrelationID = dec.DecodeInt32()
	resp.Topics = make([]ProduceRespTopic, dec.DecodeArrayLen())
	for ti := range resp.Topics {
		var t = &resp.Topics[ti]
		t.Name = dec.DecodeString()
		t.Partitions = make([]ProduceRespPartition, dec.DecodeArrayLen())
		for pi := range t.Partitions {
			var p = &t.Partitions[pi]
			p.ID = dec.DecodeInt32()
			p.Err = errFromNo(dec.DecodeInt16())
			p.Offset = dec.DecodeInt64()
		}
	}

	if err := dec.Err(); err != nil {
		return nil, err
	}
	return &resp, nil
}

type OffsetReq struct {
	CorrelationID int32
	ClientID      string
	ReplicaID     int32
	Topics        []OffsetReqTopic
}

type OffsetReqTopic struct {
	Name       string
	Partitions []OffsetReqPartition
}

type OffsetReqPartition struct {
	ID         int32
	TimeMs     int64 // cannot be time.Time because of negative values
	MaxOffsets int32
}

func ReadOffsetReq(r io.Reader) (*OffsetReq, error) {
	var req OffsetReq
	dec := NewDecoder(r)

	// total message size
	_ = dec.DecodeInt32()
	// api key + api version
	_ = dec.DecodeInt32()
	req.CorrelationID = dec.DecodeInt32()
	req.ClientID = dec.DecodeString()
	req.ReplicaID = dec.DecodeInt32()
	req.Topics = make([]OffsetReqTopic, dec.DecodeArrayLen())
	for ti := range req.Topics {
		var topic = &req.Topics[ti]
		topic.Name = dec.DecodeString()
		topic.Partitions = make([]OffsetReqPartition, dec.DecodeArrayLen())
		for pi := range topic.Partitions {
			var part = &topic.Partitions[pi]
			part.ID = dec.DecodeInt32()
			part.TimeMs = dec.DecodeInt64()
			part.MaxOffsets = dec.DecodeInt32()
		}
	}

	if dec.Err() != nil {
		return nil, dec.Err()
	}
	return &req, nil
}

func (r *OffsetReq) Bytes() ([]byte, error) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)

	// message size - for now just placeholder
	enc.Encode(int32(0))
	enc.Encode(int16(offsetReq))
	enc.Encode(int16(0))
	enc.Encode(r.CorrelationID)
	enc.Encode(r.ClientID)

	enc.Encode(r.ReplicaID)
	enc.EncodeArrayLen(len(r.Topics))
	for _, topic := range r.Topics {
		enc.Encode(topic.Name)
		enc.EncodeArrayLen(len(topic.Partitions))
		for _, part := range topic.Partitions {
			enc.Encode(part.ID)
			enc.Encode(part.TimeMs)
			enc.Encode(part.MaxOffsets)
		}
	}

	if enc.Err() != nil {
		return nil, enc.Err()
	}

	// update the message size information
	b := buf.Bytes()
	binary.BigEndian.PutUint32(b, uint32(len(b)-4))

	return b, nil
}

func (r *OffsetReq) WriteTo(w io.Writer) (int64, error) {
	b, err := r.Bytes()
	if err != nil {
		return 0, err
	}
	n, err := w.Write(b)
	return int64(n), err
}

type OffsetResp struct {
	CorrelationID int32
	Topics        []OffsetRespTopic
}

type OffsetRespTopic struct {
	Name       string
	Partitions []OffsetRespPartition
}

type OffsetRespPartition struct {
	ID      int32
	Err     error
	Offsets []int64
}

func ReadOffsetResp(r io.Reader) (*OffsetResp, error) {
	var resp OffsetResp
	dec := NewDecoder(r)

	// total message size
	_ = dec.DecodeInt32()
	resp.CorrelationID = dec.DecodeInt32()
	resp.Topics = make([]OffsetRespTopic, dec.DecodeArrayLen())
	for ti := range resp.Topics {
		var t = &resp.Topics[ti]
		t.Name = dec.DecodeString()
		t.Partitions = make([]OffsetRespPartition, dec.DecodeArrayLen())
		for pi := range t.Partitions {
			var p = &t.Partitions[pi]
			p.ID = dec.DecodeInt32()
			p.Err = errFromNo(dec.DecodeInt16())
			p.Offsets = make([]int64, dec.DecodeArrayLen())
			for oi := range p.Offsets {
				p.Offsets[oi] = dec.DecodeInt64()
			}
		}
	}

	if err := dec.Err(); err != nil {
		return nil, err
	}
	return &resp, nil
}

func (r *OffsetResp) Bytes() ([]byte, error) {
	var buf bytes.Buffer
	enc := NewEncoder(&buf)

	// message size - for now just placeholder
	enc.Encode(int32(0))
	enc.Encode(r.CorrelationID)
	enc.EncodeArrayLen(len(r.Topics))
	for _, topic := range r.Topics {
		enc.Encode(topic.Name)
		enc.EncodeArrayLen(len(topic.Partitions))
		for _, part := range topic.Partitions {
			enc.Encode(part.ID)
			enc.EncodeError(part.Err)
			enc.EncodeArrayLen(len(part.Offsets))
			for _, off := range part.Offsets {
				enc.Encode(off)
			}
		}
	}

	if enc.Err() != nil {
		return nil, enc.Err()
	}

	// update the message size information
	b := buf.Bytes()
	binary.BigEndian.PutUint32(b, uint32(len(b)-4))

	return b, nil
}
