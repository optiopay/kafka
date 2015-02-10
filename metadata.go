package kafka

import (
	"bytes"
	"encoding/binary"
	"io"
)

// MetadataReq discovers which broker is the leader for each topic partition.
type MetadataReq struct {
	CorrelationID int32
	ClientID      string
	Topics        []string
}

// MetadataResp responsds to a MetadataReq.
type MetadataResp struct {
	CorrelationID int32
	Brokers       []BrokerMetadata
	Topics        []TopicMetadata
}

// BrokerMetadata points to a Kafka broker.
type BrokerMetadata struct {
	NodeID int32
	Host   string
	Port   int32
}

// TopicMetadata provides partition data for a specific topic.
type TopicMetadata struct {
	Name       string
	Err        error
	Partitions []PartitionMetadata
}

// PartitionMetadata provides partition data for a given topic.
type PartitionMetadata struct {
	Err      error
	ID       int32
	Leader   int32
	Replicas []int32
	InSync   []int32
}

// ConsumerMetadataReq is used to discover the current offset coordinator.
type ConsumerMetadataReq struct {
	CorrelationID int32
	ClientID      string
	ConsumerGroup string
}

// ConsumerMetadataResp responds to a ConsumerMetadataReq.
type ConsumerMetadataResp struct {
	CorrelationID   int32
	Err             error
	CoordinatorID   int32
	CoordinatorHost string
	CoordinatorPort int32
}

// Bytes encodes a MetadataReq to a list of bytes
func (r *MetadataReq) Bytes() ([]byte, error) {
	var buf bytes.Buffer
	enc := newEncoder(&buf)

	enc.Encode(int32(0)) // message size
	enc.Encode(int16(reqMetadata))
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

	b := buf.Bytes()
	binary.BigEndian.PutUint32(b, uint32(len(b)-4)) // update message size
	return b, nil
}

// WriteTo converts a MetadataReq to bytes and returns a count of bytes written.
func (r *MetadataReq) WriteTo(w io.Writer) (int64, error) {
	b, err := r.Bytes()
	if err != nil {
		return 0, err
	}
	n, err := w.Write(b)
	return int64(n), err
}

// ReadMetadataResp returns a populated MetadataResp object.
func ReadMetadataResp(r io.Reader) (*MetadataResp, error) {
	var resp MetadataResp
	dec := newDecoder(r)

	_ = dec.DecodeInt32() // message size
	resp.CorrelationID = dec.DecodeInt32()

	resp.Brokers = make([]BrokerMetadata, dec.DecodeArrayLen())
	for brokerIndex := range resp.Brokers {
		var broker = &resp.Brokers[brokerIndex]
		broker.NodeID = dec.DecodeInt32()
		broker.Host = dec.DecodeString()
		broker.Port = dec.DecodeInt32()
	}

	resp.Topics = make([]TopicMetadata, dec.DecodeArrayLen())
	for topicIndex := range resp.Topics {
		var topic = &resp.Topics[topicIndex]
		topic.Err = errFromNo(dec.DecodeInt16())
		topic.Name = dec.DecodeString()

		topic.Partitions = make([]PartitionMetadata, dec.DecodeArrayLen())
		for partIndex := range topic.Partitions {
			var partition = &topic.Partitions[partIndex]
			partition.Err = errFromNo(dec.DecodeInt16())
			partition.ID = dec.DecodeInt32()
			partition.Leader = dec.DecodeInt32()

			partition.Replicas = make([]int32, dec.DecodeArrayLen())
			for replIndex := range partition.Replicas {
				partition.Replicas[replIndex] = dec.DecodeInt32()
			}

			partition.InSync = make([]int32, dec.DecodeArrayLen())
			for inSyncIndex := range partition.InSync {
				partition.InSync[inSyncIndex] = dec.DecodeInt32()
			}
		}
	}

	if dec.Err() != nil {
		return nil, dec.Err()
	}

	return &resp, nil
}

// Bytes encodes a ConsumerMetadataReq as a list of bytes.
func (r *ConsumerMetadataReq) Bytes() ([]byte, error) {
	var buf bytes.Buffer
	enc := newEncoder(&buf)

	enc.Encode(int32(0)) // message size
	enc.Encode(int16(reqConsumerMeta))
	enc.Encode(int16(0))
	enc.Encode(r.CorrelationID)
	enc.Encode(r.ClientID)

	enc.Encode(r.ConsumerGroup)

	if enc.Err() != nil {
		return nil, enc.Err()
	}

	b := buf.Bytes()
	binary.BigEndian.PutUint32(b, uint32(len(b)-4)) // update message size
	return b, nil
}

// WriteTo returns a count of the number of bytes written.
func (r *ConsumerMetadataReq) WriteTo(w io.Writer) (int64, error) {
	b, err := r.Bytes()
	if err != nil {
		return 0, err
	}
	n, err := w.Write(b)
	return int64(n), err
}

// ReadConsumerMetadataResp populates a ConsumerMetadataResp object.
func ReadConsumerMetadataResp(r io.Reader) (*ConsumerMetadataResp, error) {
	var resp ConsumerMetadataResp
	dec := newDecoder(r)

	_ = dec.DecodeInt32() // message size
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
