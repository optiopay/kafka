package kafka

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

var ErrNotEnoughData = errors.New("not enough data")

type decoder struct {
	buf []byte
	r   io.Reader
	err error
}

func newDecoder(r io.Reader) *decoder {
	return &decoder{
		r:   r,
		buf: make([]byte, 1024),
	}
}

func (d *decoder) DecodeInt8() int8 {
	if d.err != nil {
		return 0
	}
	b := d.buf[:1]
	n, err := io.ReadFull(d.r, b)
	if err != nil {
		d.err = err
		return 0
	}
	if n != 1 {
		d.err = ErrNotEnoughData
		return 0
	}
	// XXX is this correct?
	return int8(b[0])
}

func (d *decoder) DecodeInt16() int16 {
	if d.err != nil {
		return 0
	}
	b := d.buf[:2]
	n, err := io.ReadFull(d.r, b)
	if err != nil {
		d.err = err
		return 0
	}
	if n != 2 {
		d.err = ErrNotEnoughData
		return 0
	}
	return int16(binary.BigEndian.Uint16(b))
}

func (d *decoder) DecodeInt32() int32 {
	if d.err != nil {
		return 0
	}
	b := d.buf[:4]
	n, err := io.ReadFull(d.r, b)
	if err != nil {
		d.err = err
		return 0
	}
	if n != 4 {
		d.err = ErrNotEnoughData
		return 0
	}
	return int32(binary.BigEndian.Uint32(b))
}

// XXX
func (d *decoder) DecodeUint32() uint32 {
	if d.err != nil {
		return 0
	}
	b := d.buf[:4]
	n, err := io.ReadFull(d.r, b)
	if err != nil {
		d.err = err
		return 0
	}
	if n != 4 {
		d.err = ErrNotEnoughData
		return 0
	}
	return binary.BigEndian.Uint32(b)
}

func (d *decoder) DecodeInt64() int64 {
	if d.err != nil {
		return 0
	}
	b := d.buf[:8]
	n, err := io.ReadFull(d.r, b)
	if err != nil {
		d.err = err
		return 0
	}
	if n != 8 {
		d.err = ErrNotEnoughData
		return 0
	}
	return int64(binary.BigEndian.Uint64(b))
}

func (d *decoder) DecodeString() string {
	if d.err != nil {
		return ""
	}
	slen := d.DecodeInt16()
	if d.err != nil {
		return ""
	}
	if slen < 1 {
		return ""
	}

	var b []byte
	if int(slen) > len(d.buf) {
		b = make([]byte, slen)
	} else {
		b = d.buf[:int(slen)]
	}
	n, err := io.ReadFull(d.r, b)
	if err != nil {
		d.err = err
		return ""
	}
	if n != int(slen) {
		d.err = ErrNotEnoughData
		return ""
	}
	return string(b)
}

func (d *decoder) DecodeArrayLen() int {
	return int(d.DecodeInt32())
}

func (d *decoder) DecodeBytes() []byte {
	if d.err != nil {
		return nil
	}
	slen := d.DecodeInt32()
	if d.err != nil {
		return nil
	}
	if slen < 1 {
		return nil
	}

	b := make([]byte, slen)
	n, err := io.ReadFull(d.r, b)
	if err != nil {
		d.err = err
		return nil
	}
	if n != int(slen) {
		d.err = ErrNotEnoughData
		return nil
	}
	return b
}

func (d *decoder) Err() error {
	return d.err
}

type encoder struct {
	w   io.Writer
	err error
}

func newEncoder(w io.Writer) *encoder {
	return &encoder{w: w}
}

func (e *encoder) Encode(value interface{}) {
	if e.err != nil {
		return
	}
	switch val := value.(type) {
	case int8:
		_, e.err = e.w.Write([]byte{byte(val)}) // XXX is this correct?
	case int16, int32, int64, uint16, uint32, uint64:
		e.err = binary.Write(e.w, binary.BigEndian, val)
	case string:
		b := []byte(val)
		e.err = binary.Write(e.w, binary.BigEndian, int16(len(b)))
		if e.err == nil {
			e.err = binary.Write(e.w, binary.BigEndian, b)
		}
	case []byte:
		if val == nil {
			e.err = binary.Write(e.w, binary.BigEndian, int32(-1))
			return
		}
		e.err = binary.Write(e.w, binary.BigEndian, int32(len(val)))
		if e.err == nil {
			e.err = binary.Write(e.w, binary.BigEndian, val)
		}
	case []int32:
		e.EncodeArrayLen(len(val))
		for _, v := range val {
			e.Encode(v)
		}
	default:
		e.err = fmt.Errorf("cannot encode type %T", value)
	}
}

func (e *encoder) EncodeError(err error) {
	if err == nil {
		e.err = binary.Write(e.w, binary.BigEndian, int16(0))
		return
	}
	kerr, ok := err.(*KafkaError)
	if !ok {
		e.err = fmt.Errorf("cannot encode error of type %T", err)
	}

	e.err = binary.Write(e.w, binary.BigEndian, int16(kerr.errno))
}

func (e *encoder) EncodeArrayLen(length int) {
	e.Encode(int32(length))
}

func (e *encoder) Err() error {
	return e.err
}
