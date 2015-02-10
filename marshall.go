package kafka

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

var (
	ErrNotEnoughData = errors.New("not enough data")
)

// decoder holds the currently decoded information.
type decoder struct {
	buf []byte
	r   io.Reader
	err error
}

// encoder holds the writer instance to encode new bytes to.
type encoder struct {
	w   io.Writer
	err error
}

// newDecoder returns a default new instance of decoder.
func newDecoder(r io.Reader) *decoder {
	return &decoder{
		buf: make([]byte, 1024),
		r:   r,
	}
}

// newEncoder returns a default new instance of encoder.
func newEncoder(w io.Writer) *encoder {
	return &encoder{w: w}
}

// DecodeInt8 reads an int8 to the decoder instance.
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

// DecodeInt16 reads an int16 to the decoder instance.
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

// DecodeInt32 reads an int32 to the decoder instance.
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

// DecodeUint32 reads an uint32 to the decoder instance.
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

// DecodeUint64 reads an uint64 to the decoder instance.
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

// DecodeString reads a string to the decoder instance, where the first two
// bytes give the string length.
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

// DecodeArrayLen delegates to DecodeInt32.
func (d *decoder) DecodeArrayLen() int {
	return int(d.DecodeInt32())
}

// DecodeBytes reads in a number of bytes defined in the first 4 bytes.
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

// Err returns the current decoder error status.
func (d *decoder) Err() error {
	return d.err
}

// Encode writes to encoder by delegating to an appropriate type handler.
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
	default:
		e.err = fmt.Errorf("cannot encode type %T", value)
	}
}

// EncodeArrayLen delegates to the Encode function with an int32 data type.
func (e *encoder) EncodeArrayLen(length int) {
	e.Encode(int32(length))
}

// Err returns the current encoder error status.
func (e *encoder) Err() error {
	return e.err
}
