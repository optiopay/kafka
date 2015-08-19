// Copyright 2009 The Go Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

// Package crc32 implements the 32-bit cyclic redundancy check, or CRC-32,
// checksum. See http://en.wikipedia.org/wiki/Cyclic_redundancy_check for
// information.
package crc32

import (
	"hash"
	"sync"
	"unsafe"
)

// The size of a CRC-32 checksum in bytes.
const Size = 4

// Predefined polynomials.
const (
	// IEEE is by far and away the most common CRC-32 polynomial.
	// Used by ethernet (IEEE 802.3), v.42, fddi, gzip, zip, png, ...
	IEEE = 0xedb88320

	// Castagnoli's polynomial, used in iSCSI.
	// Has better error detection characteristics than IEEE.
	// http://dx.doi.org/10.1109/26.231911
	Castagnoli = 0x82f63b78

	// Koopman's polynomial.
	// Also has better error detection characteristics than IEEE.
	// http://dx.doi.org/10.1109/DSN.2002.1028931
	Koopman = 0xeb31d82e
)

// Table is a 256-word table representing the polynomial for efficient processing.
type Table [256]uint32

// castagnoliTable points to a lazily initialized Table for the Castagnoli
// polynomial. MakeTable will always return this value when asked to make a
// Castagnoli table so we can compare against it to find when the caller is
// using this polynomial.
var castagnoliTable *Table
var castagnoliOnce sync.Once

func castagnoliInit() {
	castagnoliTable = makeTable(Castagnoli)
}

// IEEETable is the table for the IEEE polynomial.
var IEEETable = makeTable(IEEE)

// slicing8Table is array of 8 Tables
type slicing8Table [8]Table

// iEEETable8 is the slicing8Table for IEEE
var iEEETable8 = makeTable8(IEEE)

// MakeTable returns the Table constructed from the specified polynomial.
func MakeTable(poly uint32) *Table {
	switch poly {
	case IEEE:
		return IEEETable
	case Castagnoli:
		castagnoliOnce.Do(castagnoliInit)
		return castagnoliTable
	}
	return makeTable(poly)
}

// makeTable returns the Table constructed from the specified polynomial.
func makeTable(poly uint32) *Table {
	t := new(Table)
	for i := 0; i < 256; i++ {
		crc := uint32(i)
		for j := 0; j < 8; j++ {
			if crc&1 == 1 {
				crc = (crc >> 1) ^ poly
			} else {
				crc >>= 1
			}
		}
		t[i] = crc
	}
	return t
}

// makeTable8 returns slicing8Table constructed from the specified polynomial.
func makeTable8(poly uint32) *slicing8Table {
	t := new(slicing8Table)
	t[0] = *makeTable(poly)
	for i := 0; i < 256; i++ {
		crc := t[0][i]
		for j := 1; j < 8; j++ {
			crc = t[0][crc&0xFF] ^ (crc >> 8)
			t[j][i] = crc
		}
	}
	return t
}

// digest represents the partial evaluation of a checksum.
type digest struct {
	crc uint32
	tab *Table
}

// New creates a new hash.Hash32 computing the CRC-32 checksum
// using the polynomial represented by the Table.
func New(tab *Table) hash.Hash32 { return &digest{0, tab} }

// NewIEEE creates a new hash.Hash32 computing the CRC-32 checksum
// using the IEEE polynomial.
func NewIEEE() hash.Hash32 { return New(IEEETable) }

func (d *digest) Size() int { return Size }

func (d *digest) BlockSize() int { return 1 }

func (d *digest) Reset() { d.crc = 0 }

func update(crc uint32, tab *Table, p []byte) uint32 {
	crc = ^crc
	for _, v := range p {
		crc = tab[byte(crc)^v] ^ (crc >> 8)
	}
	return ^crc
}

var isLittleEndian = testLittleEndian()

func testLittleEndian() bool {
	v := uintptr(1)
	bytes := (*[4]byte)(unsafe.Pointer(&v))
	return bytes[0] == 1
}

// updateSlicingBy8 updates CRC using Slicing-by-8
func updateSlicingBy8(crc uint32, tab *slicing8Table, p []byte) uint32 {
	ptr := uintptr(unsafe.Pointer(&p[0]))
	// check align by 8 bytes
	if ptr&7 > 0 {
		n := 8 - (ptr & 7)
		crc = update(crc, &tab[0], p[:n])
		p = p[n:]
	}

	crc = ^crc
	n := len(p) / 8
	pp := (*[1 << 30]uint64)(unsafe.Pointer(&p[0]))[:n]
	for _, v := range pp {
		crc ^= uint32(v & 0xFFFFFFFF)
		next := uint32(v >> 32)
		crc = tab[0][next>>24] ^ tab[1][(next>>16)&0xFF] ^
			tab[2][(next>>8)&0xFF] ^ tab[3][next&0xFF] ^
			tab[4][crc>>24] ^ tab[5][(crc>>16)&0xFF] ^
			tab[6][(crc>>8)&0xFF] ^ tab[7][crc&0xFF]
	}

	crc = ^crc
	return update(crc, &tab[0], p[n*8:])
}

// Update returns the result of adding the bytes in p to the crc.
func Update(crc uint32, tab *Table, p []byte) uint32 {
	if tab == castagnoliTable {
		return updateCastagnoli(crc, p)
	}
	if tab == IEEETable && isLittleEndian && len(p) > 64 {
		return updateSlicingBy8(crc, iEEETable8, p)
	}
	return update(crc, tab, p)
}

func (d *digest) Write(p []byte) (n int, err error) {
	d.crc = Update(d.crc, d.tab, p)
	return len(p), nil
}

func (d *digest) Sum32() uint32 { return d.crc }

func (d *digest) Sum(in []byte) []byte {
	s := d.Sum32()
	return append(in, byte(s>>24), byte(s>>16), byte(s>>8), byte(s))
}

// Checksum returns the CRC-32 checksum of data
// using the polynomial represented by the Table.
func Checksum(data []byte, tab *Table) uint32 { return Update(0, tab, data) }

// ChecksumIEEE returns the CRC-32 checksum of data
// using the IEEE polynomial.
func ChecksumIEEE(data []byte) uint32 { return Update(0, IEEETable, data) }
