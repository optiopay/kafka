package kafka

import (
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
)

const (
	reqProduce      = 0
	reqFetch        = 1
	reqOffset       = 2
	reqMetadata     = 3
	reqOffsetCommit = 8
	reqOffsetFetch  = 9
	reqConsumerMeta = 10

	compressNone   = 0
	compressGZIP   = 1
	compressSnappy = 2

	offsetLatest   = -1
	offsetEarliest = -2
)

// Message encapsualtes a Kafka message.
type Message struct {
	Offset int64
	Crc    uint32
	Key    []byte
	Value  []byte
}

// readMessageSet read in each Kafka message until EOF.
func readMessageSet(r io.Reader) ([]*Message, error) {
	set := make([]*Message, 0, 32)
	dec := newDecoder(r)
	msg := &Message{}

	var offset int64
	var attributes int8
	var err error

	for {
		offset = dec.DecodeInt64()
		if err = dec.Err(); err != nil {
			if err == io.EOF {
				return set, nil
			}
			return nil, err
		}

		_ = dec.DecodeInt32() // single message size
		msg.Offset = offset
		msg.Crc = dec.DecodeUint32() // TODO(husio) check crc
		_ = dec.DecodeInt8()         // magic byte

		attributes = dec.DecodeInt8()
		if attributes != compressNone {
			return nil, errors.New("cannot read compressed message") // TODO(husio)
		}

		msg.Key = dec.DecodeBytes()
		msg.Value = dec.DecodeBytes()
		set = append(set, msg)
	}
}

// Bytes encodes a Message to a list of bytes.
func (m *Message) Bytes() ([]byte, error) {
	var buf bytes.Buffer
	enc := newEncoder(&buf)

	enc.Encode(int32(0)) // crc placeholder
	enc.Encode(int8(0))  // magic byte is always 0
	enc.Encode(int8(0))  // no compress support
	enc.Encode(m.Key)
	enc.Encode(m.Value)

	if enc.Err() != nil {
		return nil, enc.Err()
	}

	b := buf.Bytes()
	binary.BigEndian.PutUint32(b, crc32.ChecksumIEEE(b[4:])) // update crc
	return b, nil
}
