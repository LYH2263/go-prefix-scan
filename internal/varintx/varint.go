// Package varintx encodes and decodes unsigned/signed varints (protobuf / SQLite style).
package varintx

import (
	"errors"
	"io"
)

var (
	ErrOverflow   = errors.New("varintx: overflow")
	ErrShortBuffer = errors.New("varintx: short buffer")
	ErrTruncated  = errors.New("varintx: truncated")
)

const (
	// MaxUint64Len is the maximum encoded length of a uint64 varint.
	MaxUint64Len = 10
	// MaxUint32Len is the maximum encoded length of a uint32 varint.
	MaxUint32Len = 5
)

// SizeUint64 returns the encoded size of x as a uint64 varint.
func SizeUint64(x uint64) int {
	n := 0
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}

// SizeUint32 returns the encoded size of x as a uint32 varint.
func SizeUint32(x uint32) int {
	return SizeUint64(uint64(x))
}

// EncodeUint64 appends the varint encoding of x to dst.
func EncodeUint64(dst []byte, x uint64) []byte {
	for x >= 0x80 {
		dst = append(dst, byte(x)|0x80)
		x >>= 7
	}
	return append(dst, byte(x))
}

// EncodeUint32 appends the varint encoding of x to dst.
func EncodeUint32(dst []byte, x uint32) []byte {
	return EncodeUint64(dst, uint64(x))
}

// PutUint64 writes the varint encoding of x into b; returns bytes written.
func PutUint64(b []byte, x uint64) (int, error) {
	need := SizeUint64(x)
	if len(b) < need {
		return 0, ErrShortBuffer
	}
	i := 0
	for x >= 0x80 {
		b[i] = byte(x) | 0x80
		x >>= 7
		i++
	}
	b[i] = byte(x)
	return i + 1, nil
}

// PutUint32 writes the varint encoding of x into b.
func PutUint32(b []byte, x uint32) (int, error) {
	return PutUint64(b, uint64(x))
}

// DecodeUint64 parses a varint from b; returns value, bytes consumed.
func DecodeUint64(b []byte) (uint64, int, error) {
	var x uint64
	var s uint
	for i, c := range b {
		if i == MaxUint64Len {
			return 0, 0, ErrOverflow
		}
		if c < 0x80 {
			if i == MaxUint64Len-1 && c > 1 {
				return 0, 0, ErrOverflow
			}
			return x | uint64(c)<<s, i + 1, nil
		}
		x |= uint64(c&0x7f) << s
		s += 7
	}
	return 0, 0, ErrTruncated
}

// DecodeUint32 parses a varint as uint32.
func DecodeUint32(b []byte) (uint32, int, error) {
	x, n, err := DecodeUint64(b)
	if err != nil {
		return 0, 0, err
	}
	if x > uint64(^uint32(0)) {
		return 0, 0, ErrOverflow
	}
	return uint32(x), n, nil
}

// EncodeInt64 encodes x using ZigZag then varint.
func EncodeInt64(dst []byte, x int64) []byte {
	return EncodeUint64(dst, ZigZagEncode(x))
}

// DecodeInt64 decodes a ZigZag varint.
func DecodeInt64(b []byte) (int64, int, error) {
	u, n, err := DecodeUint64(b)
	if err != nil {
		return 0, 0, err
	}
	return ZigZagDecode(u), n, nil
}

// ZigZagEncode maps signed integers to unsigned.
func ZigZagEncode(x int64) uint64 {
	return uint64((x << 1) ^ (x >> 63))
}

// ZigZagDecode reverses ZigZagEncode.
func ZigZagDecode(u uint64) int64 {
	return int64((u >> 1) ^ -(u & 1))
}

// SizeInt64 returns encoded size of a ZigZag int64.
func SizeInt64(x int64) int {
	return SizeUint64(ZigZagEncode(x))
}

// Reader reads varints from an io.ByteReader.
type Reader struct {
	r io.ByteReader
}

// NewReader wraps r.
func NewReader(r io.ByteReader) *Reader {
	return &Reader{r: r}
}

// Uint64 reads one uint64 varint.
func (r *Reader) Uint64() (uint64, error) {
	var x uint64
	var s uint
	for i := 0; i < MaxUint64Len; i++ {
		c, err := r.r.ReadByte()
		if err != nil {
			if i == 0 {
				return 0, err
			}
			return 0, ErrTruncated
		}
		if c < 0x80 {
			if i == MaxUint64Len-1 && c > 1 {
				return 0, ErrOverflow
			}
			return x | uint64(c)<<s, nil
		}
		x |= uint64(c&0x7f) << s
		s += 7
	}
	return 0, ErrOverflow
}

// Int64 reads a ZigZag int64.
func (r *Reader) Int64() (int64, error) {
	u, err := r.Uint64()
	if err != nil {
		return 0, err
	}
	return ZigZagDecode(u), nil
}

// Writer writes varints to an underlying slice builder.
type Writer struct {
	buf []byte
}

// NewWriter creates a Writer with capacity hint.
func NewWriter(capHint int) *Writer {
	return &Writer{buf: make([]byte, 0, capHint)}
}

// Reset clears the buffer.
func (w *Writer) Reset() { w.buf = w.buf[:0] }

// Bytes returns the encoded bytes.
func (w *Writer) Bytes() []byte { return w.buf }

// Uint64 appends a uint64 varint.
func (w *Writer) Uint64(x uint64) {
	w.buf = EncodeUint64(w.buf, x)
}

// Int64 appends a ZigZag int64.
func (w *Writer) Int64(x int64) {
	w.buf = EncodeInt64(w.buf, x)
}

// Uint32 appends a uint32 varint.
func (w *Writer) Uint32(x uint32) {
	w.buf = EncodeUint32(w.buf, x)
}

// Len returns buffer length.
func (w *Writer) Len() int { return len(w.buf) }

// ConsumeUint64 is like DecodeUint64 but advances a cursor pointer.
func ConsumeUint64(b []byte, off int) (uint64, int, error) {
	if off < 0 || off > len(b) {
		return 0, off, ErrShortBuffer
	}
	v, n, err := DecodeUint64(b[off:])
	if err != nil {
		return 0, off, err
	}
	return v, off + n, nil
}

// ConsumeInt64 consumes a ZigZag int64.
func ConsumeInt64(b []byte, off int) (int64, int, error) {
	u, noff, err := ConsumeUint64(b, off)
	if err != nil {
		return 0, off, err
	}
	return ZigZagDecode(u), noff, nil
}

// EncodePair encodes key-length and value-length style prefixes.
func EncodePair(dst []byte, a, b uint64) []byte {
	dst = EncodeUint64(dst, a)
	dst = EncodeUint64(dst, b)
	return dst
}

// DecodePair decodes two consecutive uint64 varints.
func DecodePair(buf []byte) (a, b uint64, n int, err error) {
	a, n1, err := DecodeUint64(buf)
	if err != nil {
		return 0, 0, 0, err
	}
	b, n2, err := DecodeUint64(buf[n1:])
	if err != nil {
		return 0, 0, 0, err
	}
	return a, b, n1 + n2, nil
}

// Skip advances over one varint without decoding the value fully into a typed result.
func Skip(b []byte) (int, error) {
	for i, c := range b {
		if i >= MaxUint64Len {
			return 0, ErrOverflow
		}
		if c < 0x80 {
			return i + 1, nil
		}
	}
	return 0, ErrTruncated
}

// MustEncodeUint64 panics on unexpected failure (for tests/static sizes).
func MustEncodeUint64(x uint64) []byte {
	return EncodeUint64(nil, x)
}

// CompareEncoded compares two encoded uint64 varints by decoded value.
func CompareEncoded(a, b []byte) (int, error) {
	va, _, err := DecodeUint64(a)
	if err != nil {
		return 0, err
	}
	vb, _, err := DecodeUint64(b)
	if err != nil {
		return 0, err
	}
	switch {
	case va < vb:
		return -1, nil
	case va > vb:
		return 1, nil
	default:
		return 0, nil
	}
}
