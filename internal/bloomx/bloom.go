// Package bloomx implements a Bloom filter with double hashing and binary serialization.
package bloomx

import (
	"encoding/binary"
	"errors"
	"math"

	"github.com/LYH2263/go-prefix-scan/internal/crc32x"
)

var (
	ErrCorrupt  = errors.New("bloomx: corrupt filter")
	ErrInvalid  = errors.New("bloomx: invalid argument")
)

const (
	magic   = uint32(0xB100F11E)
	version = uint8(1)
)

// Filter is a classic Bloom filter.
type Filter struct {
	bits   []byte
	m      uint64 // bit count
	k      uint32 // hash functions
	nAdded uint64
}

// New creates a filter sized for n elements at false-positive rate fp.
func New(n int, fp float64) *Filter {
	if n <= 0 {
		n = 1
	}
	if fp <= 0 || fp >= 1 {
		fp = 0.01
	}
	m := OptimalBits(n, fp)
	k := OptimalHashFuncs(m, n)
	nbytes := (m + 7) / 8
	return &Filter{
		bits: make([]byte, nbytes),
		m:    m,
		k:    k,
	}
}

// NewWithParams creates a filter with explicit bit count and hash count.
func NewWithParams(mBits uint64, k uint32) *Filter {
	if mBits == 0 {
		mBits = 64
	}
	if k == 0 {
		k = 4
	}
	nbytes := (mBits + 7) / 8
	return &Filter{
		bits: make([]byte, nbytes),
		m:    mBits,
		k:    k,
	}
}

// OptimalBits returns bit count for n elements and fp rate.
func OptimalBits(n int, fp float64) uint64 {
	// m = -n*ln(p) / (ln2)^2
	m := -float64(n) * math.Log(fp) / (math.Ln2 * math.Ln2)
	if m < 64 {
		m = 64
	}
	return uint64(math.Ceil(m))
}

// OptimalHashFuncs returns k for given m and n.
func OptimalHashFuncs(m uint64, n int) uint32 {
	if n <= 0 {
		return 1
	}
	k := float64(m) / float64(n) * math.Ln2
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	return uint32(math.Round(k))
}

func mix64(z uint64) uint64 {
	z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9
	z = (z ^ (z >> 27)) * 0x94d049bb133111eb
	return z ^ (z >> 31)
}

func hashPair(key []byte) (uint64, uint64) {
	h1 := uint64(crc32x.ChecksumCastagnoli(key))
	h2 := uint64(crc32x.ChecksumIEEE(key))
	if h2 == 0 {
		h2 = 0x9e3779b97f4a7c15
	}
	h1 = mix64(h1 ^ uint64(len(key))*0x9e3779b97f4a7c15)
	h2 = mix64(h2 ^ h1)
	return h1, h2
}

func (f *Filter) setBit(i uint64) {
	f.bits[i/8] |= 1 << (i % 8)
}

func (f *Filter) getBit(i uint64) bool {
	return f.bits[i/8]&(1<<(i%8)) != 0
}

// Add inserts key into the filter.
func (f *Filter) Add(key []byte) {
	if f == nil || f.m == 0 {
		return
	}
	h1, h2 := hashPair(key)
	for i := uint32(0); i < f.k; i++ {
		idx := (h1 + uint64(i)*h2) % f.m
		f.setBit(idx)
	}
	f.nAdded++
}

// MayContain reports whether key may be present (never false negative).
func (f *Filter) MayContain(key []byte) bool {
	if f == nil || f.m == 0 {
		return true
	}
	h1, h2 := hashPair(key)
	for i := uint32(0); i < f.k; i++ {
		idx := (h1 + uint64(i)*h2) % f.m
		if !f.getBit(idx) {
			return false
		}
	}
	return true
}

// Reset clears all bits.
func (f *Filter) Reset() {
	for i := range f.bits {
		f.bits[i] = 0
	}
	f.nAdded = 0
}

// NumBits returns m.
func (f *Filter) NumBits() uint64 { return f.m }

// NumHashes returns k.
func (f *Filter) NumHashes() uint32 { return f.k }

// NumAdded returns insertions count (may include duplicates).
func (f *Filter) NumAdded() uint64 { return f.nAdded }

// BitBytes returns underlying bitset length in bytes.
func (f *Filter) BitBytes() int { return len(f.bits) }

// FillRatio estimates fraction of bits set.
func (f *Filter) FillRatio() float64 {
	if len(f.bits) == 0 {
		return 0
	}
	set := 0
	for _, b := range f.bits {
		set += popcount(b)
	}
	return float64(set) / float64(f.m)
}

func popcount(x byte) int {
	n := 0
	for x != 0 {
		n += int(x & 1)
		x >>= 1
	}
	return n
}

// EstimateFP returns an approximate false-positive probability.
func (f *Filter) EstimateFP() float64 {
	if f.m == 0 || f.k == 0 {
		return 1
	}
	// (1 - e^{-kn/m})^k
	exp := -float64(f.k) * float64(f.nAdded) / float64(f.m)
	base := 1 - math.Exp(exp)
	return math.Pow(base, float64(f.k))
}

// MarshalBinary serializes the filter.
func (f *Filter) MarshalBinary() ([]byte, error) {
	if f == nil {
		return nil, ErrInvalid
	}
	out := make([]byte, 4+1+8+4+8+len(f.bits))
	binary.LittleEndian.PutUint32(out[0:4], magic)
	out[4] = version
	binary.LittleEndian.PutUint64(out[5:13], f.m)
	binary.LittleEndian.PutUint32(out[13:17], f.k)
	binary.LittleEndian.PutUint64(out[17:25], f.nAdded)
	copy(out[25:], f.bits)
	return out, nil
}

// UnmarshalBinary deserializes a filter.
func UnmarshalBinary(data []byte) (*Filter, error) {
	if len(data) < 25 {
		return nil, ErrCorrupt
	}
	if binary.LittleEndian.Uint32(data[0:4]) != magic {
		return nil, ErrCorrupt
	}
	if data[4] != version {
		return nil, ErrCorrupt
	}
	m := binary.LittleEndian.Uint64(data[5:13])
	k := binary.LittleEndian.Uint32(data[13:17])
	nAdded := binary.LittleEndian.Uint64(data[17:25])
	need := int((m + 7) / 8)
	if len(data) < 25+need {
		return nil, ErrCorrupt
	}
	bits := make([]byte, need)
	copy(bits, data[25:25+need])
	return &Filter{bits: bits, m: m, k: k, nAdded: nAdded}, nil
}

// Clone returns a deep copy.
func (f *Filter) Clone() *Filter {
	if f == nil {
		return nil
	}
	bits := make([]byte, len(f.bits))
	copy(bits, f.bits)
	return &Filter{bits: bits, m: f.m, k: f.k, nAdded: f.nAdded}
}

// Merge ORs another filter of the same shape into f.
func (f *Filter) Merge(other *Filter) error {
	if other == nil {
		return nil
	}
	if f.m != other.m || f.k != other.k || len(f.bits) != len(other.bits) {
		return ErrInvalid
	}
	for i := range f.bits {
		f.bits[i] |= other.bits[i]
	}
	f.nAdded += other.nAdded
	return nil
}

// Builder accumulates keys then builds a sized filter.
type Builder struct {
	keys [][]byte
	fp   float64
}

// NewBuilder creates a builder with target fp rate.
func NewBuilder(fp float64) *Builder {
	if fp <= 0 || fp >= 1 {
		fp = 0.01
	}
	return &Builder{fp: fp}
}

// Add records a key.
func (b *Builder) Add(key []byte) {
	cp := make([]byte, len(key))
	copy(cp, key)
	b.keys = append(b.keys, cp)
}

// Len returns number of keys recorded.
func (b *Builder) Len() int { return len(b.keys) }

// Build constructs the filter.
func (b *Builder) Build() *Filter {
	f := New(len(b.keys), b.fp)
	for _, k := range b.keys {
		f.Add(k)
	}
	return f
}

// Reset clears recorded keys.
func (b *Builder) Reset() { b.keys = b.keys[:0] }
