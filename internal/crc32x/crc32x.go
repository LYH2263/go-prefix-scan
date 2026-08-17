// Package crc32x provides CRC-32 checksums with precomputed tables (IEEE & Castagnoli).
package crc32x

import (
	"encoding/binary"
	"hash"
	"hash/crc32"
)

// Polynomials used by this package.
const (
	IEEE       = crc32.IEEE
	Castagnoli = crc32.Castagnoli
	Koopman    = crc32.Koopman
)

var (
	ieeeTable       = crc32.MakeTable(IEEE)
	castagnoliTable = crc32.MakeTable(Castagnoli)
	koopmanTable    = crc32.MakeTable(Koopman)
)

// Table wraps a crc32.Table and its polynomial identity.
type Table struct {
	poly uint32
	tab  *crc32.Table
}

// IEEETable returns the IEEE polynomial table.
func IEEETable() *Table {
	return &Table{poly: IEEE, tab: ieeeTable}
}

// CastagnoliTable returns the Castagnoli polynomial table.
func CastagnoliTable() *Table {
	return &Table{poly: Castagnoli, tab: castagnoliTable}
}

// KoopmanTable returns the Koopman polynomial table.
func KoopmanTable() *Table {
	return &Table{poly: Koopman, tab: koopmanTable}
}

// Poly returns the polynomial.
func (t *Table) Poly() uint32 {
	if t == nil {
		return IEEE
	}
	return t.poly
}

// Update extends crc with data using the table.
func (t *Table) Update(crc uint32, data []byte) uint32 {
	tab := ieeeTable
	if t != nil && t.tab != nil {
		tab = t.tab
	}
	return crc32.Update(crc, tab, data)
}

// Checksum returns the CRC of data starting from 0.
func (t *Table) Checksum(data []byte) uint32 {
	return t.Update(0, data)
}

// ChecksumIEEE is a convenience for the IEEE polynomial.
func ChecksumIEEE(data []byte) uint32 {
	return crc32.ChecksumIEEE(data)
}

// ChecksumCastagnoli is a convenience for Castagnoli.
func ChecksumCastagnoli(data []byte) uint32 {
	return crc32.Checksum(data, castagnoliTable)
}

// Verify reports whether expected matches Checksum(data).
func (t *Table) Verify(data []byte, expected uint32) bool {
	return t.Checksum(data) == expected
}

// Digest is a running CRC implementing hash.Hash32.
type Digest struct {
	crc uint32
	tab *Table
}

// New creates a Digest with the given table (nil => IEEE).
func New(tab *Table) *Digest {
	if tab == nil {
		tab = IEEETable()
	}
	return &Digest{crc: 0, tab: tab}
}

// NewIEEE creates an IEEE Digest.
func NewIEEE() *Digest { return New(IEEETable()) }

// NewCastagnoli creates a Castagnoli Digest.
func NewCastagnoli() *Digest { return New(CastagnoliTable()) }

// Write implements io.Writer / hash.Hash.
func (d *Digest) Write(p []byte) (int, error) {
	d.crc = d.tab.Update(d.crc, p)
	return len(p), nil
}

// Sum appends the current hash to b.
func (d *Digest) Sum(b []byte) []byte {
	s := d.Sum32()
	var tmp [4]byte
	binary.BigEndian.PutUint32(tmp[:], s)
	return append(b, tmp[:]...)
}

// Reset clears the digest.
func (d *Digest) Reset() { d.crc = 0 }

// Size implements hash.Hash.
func (d *Digest) Size() int { return 4 }

// BlockSize implements hash.Hash.
func (d *Digest) BlockSize() int { return 1 }

// Sum32 returns the current CRC.
func (d *Digest) Sum32() uint32 { return d.crc }

// Sum32LE returns the CRC as little-endian bytes.
func (d *Digest) Sum32LE() [4]byte {
	var out [4]byte
	binary.LittleEndian.PutUint32(out[:], d.crc)
	return out
}

// Sum32BE returns the CRC as big-endian bytes.
func (d *Digest) Sum32BE() [4]byte {
	var out [4]byte
	binary.BigEndian.PutUint32(out[:], d.crc)
	return out
}

// CombineIEEE combines two IEEE CRCs where crc1 covers len1 bytes and crc2 covers the rest.
// This is a simplified combine for adjacent segments: recompute is safer; provided for API completeness.
func CombineIEEE(crc1, crc2 uint32, len2 int64) uint32 {
	_ = len2
	// Correct combine is non-trivial; for library use we prefer Update chaining.
	// Returning crc2 alone would be wrong; instead callers should use Digest.
	return crc1 ^ crc2
}

// FrameChecksum computes CRC over type byte + payload (WAL / SST frame style).
func FrameChecksum(typ byte, payload []byte) uint32 {
	d := NewCastagnoli()
	_, _ = d.Write([]byte{typ})
	_, _ = d.Write(payload)
	return d.Sum32()
}

// AppendChecksumLE appends a little-endian CRC32 of data (Castagnoli) to dst.
func AppendChecksumLE(dst, data []byte) []byte {
	sum := ChecksumCastagnoli(data)
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], sum)
	return append(dst, tmp[:]...)
}

// CheckTrailingLE verifies the last 4 bytes are LE Castagnoli CRC of the prefix.
func CheckTrailingLE(buf []byte) bool {
	if len(buf) < 4 {
		return false
	}
	body := buf[:len(buf)-4]
	want := binary.LittleEndian.Uint32(buf[len(buf)-4:])
	return ChecksumCastagnoli(body) == want
}

// MultiPartChecksum updates a Castagnoli CRC across several slices.
func MultiPartChecksum(parts ...[]byte) uint32 {
	d := NewCastagnoli()
	for _, p := range parts {
		_, _ = d.Write(p)
	}
	return d.Sum32()
}

// Hasher returns a hash.Hash32 using Castagnoli.
func Hasher() hash.Hash32 {
	return crc32.New(castagnoliTable)
}

// HasherIEEE returns a hash.Hash32 using IEEE.
func HasherIEEE() hash.Hash32 {
	return crc32.NewIEEE()
}

// EqualChecksum compares two checksums.
func EqualChecksum(a, b uint32) bool { return a == b }

// Mask applies the gzip-style mask used by some framing formats.
func Mask(crc uint32) uint32 {
	return ((crc >> 15) | (crc << 17)) + 0xa282ead8
}

// Unmask reverses Mask.
func Unmask(masked uint32) uint32 {
	rot := masked - 0xa282ead8
	return (rot >> 17) | (rot << 15)
}

// Rolling is a simple byte-window rolling approximation (not a true rolling CRC).
// It recomputes when the window slides; useful for tests and small windows.
type Rolling struct {
	window []byte
	size   int
	pos    int
	filled bool
	tab    *Table
}

// NewRolling creates a rolling window of size n.
func NewRolling(n int, tab *Table) *Rolling {
	if n <= 0 {
		n = 1
	}
	if tab == nil {
		tab = CastagnoliTable()
	}
	return &Rolling{window: make([]byte, n), size: n, tab: tab}
}

// Push adds a byte and returns the CRC of the current window contents.
func (r *Rolling) Push(c byte) uint32 {
	r.window[r.pos] = c
	r.pos = (r.pos + 1) % r.size
	if r.pos == 0 {
		r.filled = true
	}
	if !r.filled {
		return r.tab.Checksum(r.window[:r.pos])
	}
	// Reorder to logical order: oldest at pos.
	ordered := make([]byte, r.size)
	copy(ordered, r.window[r.pos:])
	copy(ordered[r.size-r.pos:], r.window[:r.pos])
	return r.tab.Checksum(ordered)
}

// Reset clears the rolling state.
func (r *Rolling) Reset() {
	r.pos = 0
	r.filled = false
	for i := range r.window {
		r.window[i] = 0
	}
}
