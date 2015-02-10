package kafka

import (
	"bytes"
	"encoding/binary"
	"io"
	"time"
)

// ProduceReq is used to send a set of new messages to Kafka.
type ProduceReq struct {
	CorrelationID int32
	ClientID      string
	RequiredAcks  int16
	Timeout       time.Duration
	Topics        []ProduceReqTopic
}

// ProduceReqTopic points to a Kafka topic.
type ProduceReqTopic struct {
	Name       string
	Partitions []ProduceReqPartition
}

// ProduceReqPartition provides a list of Messages to write to a partition.
type ProduceReqPartition struct {
	Partition int32
	Messages  []*Message
}

// ProduceResp responds to a ProduceReq.
type ProduceResp struct {
	CorrelationID int32
	Topics        []ProduceRespTopic
}

// ProduceRespTopic points to a Kafka topic.
type ProduceRespTopic struct {
	Name       string
	Partitions []ProduceRespPartition
}

// ProduceRespPartition points to a partition offset.
type ProduceRespPartition struct {
	Partition int32
	Err       error
	Offset    int64
}

// Bytes encodes a ProduceReq as a list of bytes.
func (r *ProduceReq) Bytes() ([]byte, error) {
	var buf bytes.Buffer
	enc := newEncoder(&buf)
	lenOffset := 8
	lenMsgSize := 4

	enc.Encode(int32(0)) // message size
	enc.Encode(int16(reqProduce))
	enc.Encode(int16(0))
	enc.Encode(r.CorrelationID)
	enc.Encode(r.ClientID)
	enc.Encode(r.RequiredAcks)
	enc.Encode(int32(r.Timeout / time.Millisecond))

	enc.EncodeArrayLen(len(r.Topics))
	for _, topic := range r.Topics {
		enc.Encode(topic.Name)
		enc.EncodeArrayLen(len(topic.Partitions))
		for _, part := range topic.Partitions {
			enc.Encode(part.Partition)

			var msgSetSize int32
			var messageBytes = make([][]byte, len(part.Messages))
			for msgIndex, msg := range part.Messages {
				b, err := msg.Bytes()
				if err != nil {
					return nil, err
				}
				messageBytes[msgIndex] = b
				msgSetSize += int32(lenOffset + lenMsgSize + len(b))
			}

			enc.Encode(msgSetSize)
			for _, b := range messageBytes {
				enc.Encode(int64(0)) // offset does not matter when producing
				enc.Encode(b)
			}
		}
	}

	if enc.Err() != nil {
		return nil, enc.Err()
	}

	b := buf.Bytes()
	binary.BigEndian.PutUint32(b, uint32(len(b)-4)) // update message size
	return b, nil
}

// WriteTo write a ProduceReq, returning the number of bytes written.
func (r *ProduceReq) WriteTo(w io.Writer) (int64, error) {
	b, err := r.Bytes()
	if err != nil {
		return 0, err
	}
	n, err := w.Write(b)
	return int64(n), err
}

// ReadProduceResp decodes the reader to a new ProduceResp object.
func ReadProduceResp(r io.Reader) (*ProduceResp, error) {
	var resp ProduceResp
	dec := newDecoder(r)

	_ = dec.DecodeInt32() // message size
	resp.CorrelationID = dec.DecodeInt32()

	resp.Topics = make([]ProduceRespTopic, dec.DecodeArrayLen())
	for topicIndex := range resp.Topics {
		var topic = &resp.Topics[topicIndex]
		topic.Name = dec.DecodeString()
		topic.Partitions = make([]ProduceRespPartition, dec.DecodeArrayLen())
		for partIndex := range topic.Partitions {
			var part = &topic.Partitions[partIndex]
			part.Partition = dec.DecodeInt32()
			part.Err = errFromNo(dec.DecodeInt16())
			part.Offset = dec.DecodeInt64()
		}
	}

	if err := dec.Err(); err != nil {
		return nil, err
	}

	return &resp, nil
}
