package proto

import (
	"bytes"

	. "gopkg.in/check.v1"
)

var _ = Suite(&SnappySuite{})

type SnappySuite struct{}

var snappyChunk = []byte("\x03\x08foo") // snappy encoding of "foo"

func (s *SnappySuite) TestSnappyDecodeNormal(c *C) {
	got, err := snappyDecode(snappyChunk)
	if err != nil {
		c.Fatal(err)
	}
	if want := []byte("foo"); !bytes.Equal(got, want) {
		c.Fatalf("got: %v; want: %v", got, want)
	}
}

func (s *SnappySuite) TestSnappyDecodeJava(c *C) {
	javafied := []byte{
		0x82, 'S', 'N', 'A', 'P', 'P', 'Y', 0x0, // magic
		0, 0, 0, 1, // version
		0, 0, 0, 1, // compatible version
		0, 0, 0, 5, // chunk size
		0x3, 0x8, 'f', 'o', 'o', // chunk data
		0, 0, 0, 5, // chunk size
		0x3, 0x8, 'f', 'o', 'o', // chunk data
	}
	got, err := snappyDecode(javafied)
	if err != nil {
		c.Fatal(err)
	}
	if want := []byte("foofoo"); !bytes.Equal(got, want) {
		c.Fatalf("got: %v; want: %v", got, want)
	}
}
