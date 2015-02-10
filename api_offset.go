package kafka

import (
	"bytes"
	"encoding/binary"
	"io"
	"time"
)

// OffsetReq describes the valid offset range for a set of topic-partitions.
type OffsetReq struct {
	CorrelationID int32
	ClientID      string
	ReplicaID     int32
	Topics        []OffsetReqTopic
}

// OffsetReqTopic points to a specific topic and list of partitions.
type OffsetReqTopic struct {
	Name       string
	Partitions []OffsetReqPartition
}

// OffsetReqPartition points to a specific offset in a partition.
type OffsetReqPartition struct {
	Partition  int32
	TimeMs     int64 // cannot be time.Time because of negative values
	MaxOffsets int32
}

// OffsetResp responds to an OffsetReq.
type OffsetResp struct {
	CorrelationID int32
	Topics        []OffsetRespTopic
}

// OffsetRespTopic points to a topic name and list of partitions.
type OffsetRespTopic struct {
	Name       string
	Partitions []OffsetRespPartition
}

// OffsetRespPartition points to a specfic offset in a partition.
type OffsetRespPartition struct {
	Partition int32
	Err       error
	Offsets   []int64
}

// OffsetCommitReq is a request to commit offset information to Kafka.
type OffsetCommitReq struct {
	CorrelationID int32
	ClientID      string
	ConsumerGroup string
	Topics        []OffsetCommitReqTopic
}

// OffsetCommitReqTopic points to a Kafka topic.
type OffsetCommitReqTopic struct {
	Name       string
	Partitions []OffsetCommitReqPartition
}

// OffsetCommitReqPartition points to a specfic offset in a partition.
type OffsetCommitReqPartition struct {
	Partition int32
	Offset    int64
	TimeStamp time.Time
	Metadata  string
}

// OffsetCommitResp responds to a OffsetCommitReq.
type OffsetCommitResp struct {
	CorrelationID int32
	Topics        []OffsetCommitRespTopic
}

// OffsetCommitRespTopic points to a Kafka topic.
type OffsetCommitRespTopic struct {
	Name       string
	Partitions []OffsetCommitRespPartition
}

// OffsetCommitRespPartition points to a topic partition.
type OffsetCommitRespPartition struct {
	Partition int32
	Err       error
}

// OffsetFetchReq requests the retrieval of offset information from the leader
// partition.
type OffsetFetchReq struct {
	CorrelationID int32
	ClientID      string
	ConsumerGroup string
	Topics        []OffsetFetchReqTopic
}

// OffsetFetchReqTopic points to a topic and list of partitions.
type OffsetFetchReqTopic struct {
	Name       string
	Partitions []int32
}

// OffsetFetchResp responds to a OffsetFetchReq.
type OffsetFetchResp struct {
	CorrelationID int32
	Topics        []OffsetFetchRespTopic
}

// OffsetFetchRespTopic points to a topic and list of partitions.
type OffsetFetchRespTopic struct {
	Name       string
	Partitions []OffsetFetchRespPartition
}

// OffsetFetchRespPartition points to a specific partition offset.
type OffsetFetchRespPartition struct {
	Partition int32
	Offset    int64
	Metadata  string
	Err       error
}

// Bytes converts an OffsetReq to a list of bytes.
func (r *OffsetReq) Bytes() ([]byte, error) {
	var buf bytes.Buffer
	enc := newEncoder(&buf)

	enc.Encode(int32(0)) // message size
	enc.Encode(int16(reqOffset))
	enc.Encode(int16(0))
	enc.Encode(r.CorrelationID)
	enc.Encode(r.ClientID)

	enc.Encode(r.ReplicaID)
	enc.EncodeArrayLen(len(r.Topics))
	for _, topic := range r.Topics {
		enc.Encode(topic.Name)
		enc.EncodeArrayLen(len(topic.Partitions))
		for _, part := range topic.Partitions {
			enc.Encode(part.Partition)
			enc.Encode(part.TimeMs)
			enc.Encode(part.MaxOffsets)
		}
	}

	if enc.Err() != nil {
		return nil, enc.Err()
	}

	b := buf.Bytes()
	binary.BigEndian.PutUint32(b, uint32(len(b)-4)) // update message size
	return b, nil
}

// WriteTo writes an OffsetReq, returning the number of bytes written.
func (r *OffsetReq) WriteTo(w io.Writer) (int64, error) {
	b, err := r.Bytes()
	if err != nil {
		return 0, err
	}
	n, err := w.Write(b)
	return int64(n), err
}

// ReadOffsetResp decodes the reader to a new OffsetResp object.
func ReadOffsetResp(r io.Reader) (*OffsetResp, error) {
	var resp OffsetResp
	dec := newDecoder(r)

	_ = dec.DecodeInt32() // message size
	resp.CorrelationID = dec.DecodeInt32()
	resp.Topics = make([]OffsetRespTopic, dec.DecodeArrayLen())
	for topicIndex := range resp.Topics {
		var topic = &resp.Topics[topicIndex]
		topic.Name = dec.DecodeString()
		topic.Partitions = make([]OffsetRespPartition, dec.DecodeArrayLen())
		for partIndex := range topic.Partitions {
			var part = &topic.Partitions[partIndex]
			part.Partition = dec.DecodeInt32()
			part.Err = errFromNo(dec.DecodeInt16())
			part.Offsets = make([]int64, dec.DecodeArrayLen())
			for offsetIndex := range part.Offsets {
				part.Offsets[offsetIndex] = dec.DecodeInt64()
			}
		}
	}

	if err := dec.Err(); err != nil {
		return nil, err
	}

	return &resp, nil
}

// Bytes converts an OffsetCommitReq to a list of bytes.
func (r *OffsetCommitReq) Bytes() ([]byte, error) {
	var buf bytes.Buffer
	enc := newEncoder(&buf)

	enc.Encode(int32(0)) // message size
	enc.Encode(int16(reqOffsetCommit))
	enc.Encode(int16(0))
	enc.Encode(r.CorrelationID)
	enc.Encode(r.ClientID)
	enc.Encode(r.ConsumerGroup)

	enc.EncodeArrayLen(len(r.Topics))
	for _, topic := range r.Topics {
		enc.Encode(topic.Name)
		enc.EncodeArrayLen(len(topic.Partitions))
		for _, part := range topic.Partitions {
			enc.Encode(part.Partition)
			enc.Encode(part.Offset)
			// TODO(husio) is this really in milliseconds?
			enc.Encode(part.TimeStamp.UnixNano() / int64(time.Millisecond))
			enc.Encode(part.Metadata)
		}
	}

	if enc.Err() != nil {
		return nil, enc.Err()
	}

	b := buf.Bytes()
	binary.BigEndian.PutUint32(b, uint32(len(b)-4)) // update message size
	return b, nil
}

// WriteTo writes an OffsetCommitReq, returning the number of bytes written.
func (r *OffsetCommitReq) WriteTo(w io.Writer) (int64, error) {
	b, err := r.Bytes()
	if err != nil {
		return 0, err
	}
	n, err := w.Write(b)
	return int64(n), err
}

// ReadOffsetCommitResp decodes the reader to an OffsetCommitResp.
func ReadOffsetCommitResp(r io.Reader) (*OffsetCommitResp, error) {
	var resp OffsetCommitResp
	dec := newDecoder(r)

	_ = dec.DecodeInt32() // message size
	resp.CorrelationID = dec.DecodeInt32()
	resp.Topics = make([]OffsetCommitRespTopic, dec.DecodeArrayLen())
	for topicIndex := range resp.Topics {
		var topic = &resp.Topics[topicIndex]
		topic.Name = dec.DecodeString()
		topic.Partitions = make([]OffsetCommitRespPartition, dec.DecodeArrayLen())
		for partIndex := range topic.Partitions {
			var part = &topic.Partitions[partIndex]
			part.Partition = dec.DecodeInt32()
			part.Err = errFromNo(dec.DecodeInt16())
		}
	}

	if err := dec.Err(); err != nil {
		return nil, err
	}

	return &resp, nil
}

// Bytes converts an OffsetFetchReq to a list of bytes.
func (r *OffsetFetchReq) Bytes() ([]byte, error) {
	var buf bytes.Buffer
	enc := newEncoder(&buf)

	enc.Encode(int32(0)) // message size
	enc.Encode(int16(reqOffsetFetch))
	enc.Encode(int16(0))
	enc.Encode(r.CorrelationID)
	enc.Encode(r.ClientID)
	enc.Encode(r.ConsumerGroup)

	enc.EncodeArrayLen(len(r.Topics))
	for _, topic := range r.Topics {
		enc.Encode(topic.Name)
		enc.EncodeArrayLen(len(topic.Partitions))
		for _, partition := range topic.Partitions {
			enc.Encode(partition)
		}
	}

	if enc.Err() != nil {
		return nil, enc.Err()
	}

	b := buf.Bytes()
	binary.BigEndian.PutUint32(b, uint32(len(b)-4)) // update message size
	return b, nil
}

// WriteTo writes an OffsetFetchReq, returning the number of bytes written.
func (r *OffsetFetchReq) WriteTo(w io.Writer) (int64, error) {
	b, err := r.Bytes()
	if err != nil {
		return 0, err
	}
	n, err := w.Write(b)
	return int64(n), err
}

// ReadOffsetFetchResp decodes a new OffsetFetchResp.
func ReadOffsetFetchResp(r io.Reader) (*OffsetFetchResp, error) {
	var resp OffsetFetchResp
	dec := newDecoder(r)

	_ = dec.DecodeInt32() // message size
	resp.CorrelationID = dec.DecodeInt32()

	resp.Topics = make([]OffsetFetchRespTopic, dec.DecodeArrayLen())
	for topicIndex := range resp.Topics {
		var topic = &resp.Topics[topicIndex]
		topic.Name = dec.DecodeString()
		topic.Partitions = make([]OffsetFetchRespPartition, dec.DecodeArrayLen())
		for partIndex := range topic.Partitions {
			var part = &topic.Partitions[partIndex]
			part.Partition = dec.DecodeInt32()
			part.Offset = dec.DecodeInt64()
			part.Metadata = dec.DecodeString()
			part.Err = errFromNo(dec.DecodeInt16())
		}
	}

	if err := dec.Err(); err != nil {
		return nil, err
	}

	return &resp, nil
}
