// Package byteutil provides low-level byte slice helpers used across the LSM stack.
package byteutil

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"unicode/utf8"
)

var (
	ErrShortBuffer = errors.New("byteutil: short buffer")
	ErrOverflow    = errors.New("byteutil: overflow")
)

// Compare is bytes.Compare with an explicit name for callers.
func Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

// Equal reports whether a and b are identical.
func Equal(a, b []byte) bool {
	return bytes.Equal(a, b)
}

// Clone returns a copy of b; nil in yields nil out.
func Clone(b []byte) []byte {
	if b == nil {
		return nil
	}
	out := make([]byte, len(b))
	copy(out, b)
	return out
}

// CloneN copies min(n, len(b)) bytes.
func CloneN(b []byte, n int) []byte {
	if n <= 0 {
		return nil
	}
	if n > len(b) {
		n = len(b)
	}
	out := make([]byte, n)
	copy(out, b[:n])
	return out
}

// Concat concatenates slices into a newly allocated buffer.
func Concat(parts ...[]byte) []byte {
	n := 0
	for _, p := range parts {
		n += len(p)
	}
	out := make([]byte, 0, n)
	for _, p := range parts {
		out = append(out, p...)
	}
	return out
}

// AppendGrow appends src to dst, growing capacity with a 1.5x policy when needed.
func AppendGrow(dst, src []byte) []byte {
	need := len(dst) + len(src)
	if cap(dst) >= need {
		return append(dst, src...)
	}
	capNew := cap(dst) * 3 / 2
	if capNew < need {
		capNew = need
	}
	if capNew < 64 {
		capNew = 64
	}
	n := make([]byte, len(dst), capNew)
	copy(n, dst)
	return append(n, src...)
}

// PutU16LE writes a little-endian uint16.
func PutU16LE(b []byte, v uint16) error {
	if len(b) < 2 {
		return ErrShortBuffer
	}
	binary.LittleEndian.PutUint16(b, v)
	return nil
}

// PutU32LE writes a little-endian uint32.
func PutU32LE(b []byte, v uint32) error {
	if len(b) < 4 {
		return ErrShortBuffer
	}
	binary.LittleEndian.PutUint32(b, v)
	return nil
}

// PutU64LE writes a little-endian uint64.
func PutU64LE(b []byte, v uint64) error {
	if len(b) < 8 {
		return ErrShortBuffer
	}
	binary.LittleEndian.PutUint64(b, v)
	return nil
}

// U16LE reads a little-endian uint16.
func U16LE(b []byte) (uint16, error) {
	if len(b) < 2 {
		return 0, ErrShortBuffer
	}
	return binary.LittleEndian.Uint16(b), nil
}

// U32LE reads a little-endian uint32.
func U32LE(b []byte) (uint32, error) {
	if len(b) < 4 {
		return 0, ErrShortBuffer
	}
	return binary.LittleEndian.Uint32(b), nil
}

// U64LE reads a little-endian uint64.
func U64LE(b []byte) (uint64, error) {
	if len(b) < 8 {
		return 0, ErrShortBuffer
	}
	return binary.LittleEndian.Uint64(b), nil
}

// PutU16BE writes a big-endian uint16.
func PutU16BE(b []byte, v uint16) error {
	if len(b) < 2 {
		return ErrShortBuffer
	}
	binary.BigEndian.PutUint16(b, v)
	return nil
}

// PutU32BE writes a big-endian uint32.
func PutU32BE(b []byte, v uint32) error {
	if len(b) < 4 {
		return ErrShortBuffer
	}
	binary.BigEndian.PutUint32(b, v)
	return nil
}

// PutU64BE writes a big-endian uint64.
func PutU64BE(b []byte, v uint64) error {
	if len(b) < 8 {
		return ErrShortBuffer
	}
	binary.BigEndian.PutUint64(b, v)
	return nil
}

// U16BE reads a big-endian uint16.
func U16BE(b []byte) (uint16, error) {
	if len(b) < 2 {
		return 0, ErrShortBuffer
	}
	return binary.BigEndian.Uint16(b), nil
}

// U32BE reads a big-endian uint32.
func U32BE(b []byte) (uint32, error) {
	if len(b) < 4 {
		return 0, ErrShortBuffer
	}
	return binary.BigEndian.Uint32(b), nil
}

// U64BE reads a big-endian uint64.
func U64BE(b []byte) (uint64, error) {
	if len(b) < 8 {
		return 0, ErrShortBuffer
	}
	return binary.BigEndian.Uint64(b), nil
}

// SharedPrefixLen returns the length of the common prefix of a and b.
func SharedPrefixLen(a, b []byte) int {
	n := len(a)
	if len(b) < n {
		n = len(b)
	}
	i := 0
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

// HasPrefix reports whether b starts with prefix.
func HasPrefix(b, prefix []byte) bool {
	return bytes.HasPrefix(b, prefix)
}

// HasSuffix reports whether b ends with suffix.
func HasSuffix(b, suffix []byte) bool {
	return bytes.HasSuffix(b, suffix)
}

// IsASCII reports whether all bytes are < 0x80.
func IsASCII(b []byte) bool {
	for _, c := range b {
		if c >= 0x80 {
			return false
		}
	}
	return true
}

// ValidUTF8 reports whether b is valid UTF-8.
func ValidUTF8(b []byte) bool {
	return utf8.Valid(b)
}

// PadRight pads b with pad to at least n bytes.
func PadRight(b []byte, n int, pad byte) []byte {
	if len(b) >= n {
		return Clone(b)
	}
	out := make([]byte, n)
	copy(out, b)
	for i := len(b); i < n; i++ {
		out[i] = pad
	}
	return out
}

// TrimRightZeros removes trailing zero bytes.
func TrimRightZeros(b []byte) []byte {
	i := len(b)
	for i > 0 && b[i-1] == 0 {
		i--
	}
	return Clone(b[:i])
}

// Fill sets all bytes of b to v.
func Fill(b []byte, v byte) {
	for i := range b {
		b[i] = v
	}
}

// XORInto writes a[i]^b[i] into dst; lengths must match.
func XORInto(dst, a, b []byte) error {
	if len(a) != len(b) || len(dst) < len(a) {
		return ErrShortBuffer
	}
	for i := range a {
		dst[i] = a[i] ^ b[i]
	}
	return nil
}

// ReadFullExact reads exactly n bytes from r.
func ReadFullExact(r io.Reader, n int) ([]byte, error) {
	if n < 0 {
		return nil, ErrOverflow
	}
	buf := make([]byte, n)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

// WriteAll writes b entirely to w.
func WriteAll(w io.Writer, b []byte) error {
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return err
		}
		b = b[n:]
	}
	return nil
}

// MinBytes returns the lexicographically smaller slice (or a if equal).
func MinBytes(a, b []byte) []byte {
	if Compare(a, b) <= 0 {
		return a
	}
	return b
}

// MaxBytes returns the lexicographically larger slice (or a if equal).
func MaxBytes(a, b []byte) []byte {
	if Compare(a, b) >= 0 {
		return a
	}
	return b
}

// InRange reports whether key is in [lo, hi) with nil meaning unbounded.
func InRange(key, lo, hi []byte) bool {
	if lo != nil && Compare(key, lo) < 0 {
		return false
	}
	if hi != nil && Compare(key, hi) >= 0 {
		return false
	}
	return true
}

// KeySuccessor returns a key that is strictly greater than k by appending 0x00,
// or nil if allocation fails conceptually (never). Used for exclusive upper bounds.
func KeySuccessor(k []byte) []byte {
	out := make([]byte, len(k)+1)
	copy(out, k)
	out[len(k)] = 0
	return out
}

// EstimateSize returns an approximate serialized size for length-prefixed blobs.
func EstimateSize(parts ...[]byte) int {
	n := 0
	for _, p := range parts {
		n += 4 + len(p)
	}
	return n
}
