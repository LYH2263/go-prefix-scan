package byteutil

import (
	"io"
	"sync"
)

// Buffer is a growable byte buffer with Reset and Bytes views.
type Buffer struct {
	buf []byte
}

// NewBuffer returns a Buffer with the given initial capacity.
func NewBuffer(capHint int) *Buffer {
	if capHint < 0 {
		capHint = 0
	}
	return &Buffer{buf: make([]byte, 0, capHint)}
}

// Reset truncates length to zero without releasing capacity.
func (b *Buffer) Reset() {
	b.buf = b.buf[:0]
}

// Len returns the current length.
func (b *Buffer) Len() int { return len(b.buf) }

// Cap returns the capacity.
func (b *Buffer) Cap() int { return cap(b.buf) }

// Bytes returns the underlying slice; valid until the next mutation.
func (b *Buffer) Bytes() []byte { return b.buf }

// CloneBytes returns an owned copy of the contents.
func (b *Buffer) CloneBytes() []byte { return Clone(b.buf) }

// Grow ensures capacity for at least n additional bytes.
func (b *Buffer) Grow(n int) {
	if n <= 0 {
		return
	}
	need := len(b.buf) + n
	if cap(b.buf) >= need {
		return
	}
	capNew := cap(b.buf) * 2
	if capNew < need {
		capNew = need
	}
	if capNew < 64 {
		capNew = 64
	}
	nb := make([]byte, len(b.buf), capNew)
	copy(nb, b.buf)
	b.buf = nb
}

// Write appends p; implements io.Writer.
func (b *Buffer) Write(p []byte) (int, error) {
	b.buf = AppendGrow(b.buf, p)
	return len(p), nil
}

// WriteByte appends a single byte.
func (b *Buffer) WriteByte(c byte) error {
	b.Grow(1)
	b.buf = append(b.buf, c)
	return nil
}

// WriteString appends s.
func (b *Buffer) WriteString(s string) (int, error) {
	b.buf = AppendGrow(b.buf, []byte(s))
	return len(s), nil
}

// WriteU16LE appends a little-endian uint16.
func (b *Buffer) WriteU16LE(v uint16) {
	var tmp [2]byte
	_ = PutU16LE(tmp[:], v)
	_, _ = b.Write(tmp[:])
}

// WriteU32LE appends a little-endian uint32.
func (b *Buffer) WriteU32LE(v uint32) {
	var tmp [4]byte
	_ = PutU32LE(tmp[:], v)
	_, _ = b.Write(tmp[:])
}

// WriteU64LE appends a little-endian uint64.
func (b *Buffer) WriteU64LE(v uint64) {
	var tmp [8]byte
	_ = PutU64LE(tmp[:], v)
	_, _ = b.Write(tmp[:])
}

// WriteU16BE appends a big-endian uint16.
func (b *Buffer) WriteU16BE(v uint16) {
	var tmp [2]byte
	_ = PutU16BE(tmp[:], v)
	_, _ = b.Write(tmp[:])
}

// WriteU32BE appends a big-endian uint32.
func (b *Buffer) WriteU32BE(v uint32) {
	var tmp [4]byte
	_ = PutU32BE(tmp[:], v)
	_, _ = b.Write(tmp[:])
}

// WriteU64BE appends a big-endian uint64.
func (b *Buffer) WriteU64BE(v uint64) {
	var tmp [8]byte
	_ = PutU64BE(tmp[:], v)
	_, _ = b.Write(tmp[:])
}

// WriteLP32 appends a uint32 length prefix (LE) followed by data.
func (b *Buffer) WriteLP32(data []byte) {
	b.WriteU32LE(uint32(len(data)))
	_, _ = b.Write(data)
}

// Truncate shrinks the buffer to n bytes if n < Len.
func (b *Buffer) Truncate(n int) {
	if n < 0 {
		n = 0
	}
	if n > len(b.buf) {
		n = len(b.buf)
	}
	b.buf = b.buf[:n]
}

// Pool is a sync.Pool of Buffers.
type Pool struct {
	p sync.Pool
}

// NewPool creates a Buffer pool with the given default capacity hint.
func NewPool(capHint int) *Pool {
	return &Pool{p: sync.Pool{
		New: func() any {
			return NewBuffer(capHint)
		},
	}}
}

// Get returns a reset Buffer from the pool.
func (p *Pool) Get() *Buffer {
	b := p.p.Get().(*Buffer)
	b.Reset()
	return b
}

// Put returns a Buffer to the pool.
func (p *Pool) Put(b *Buffer) {
	if b == nil {
		return
	}
	if b.Cap() > 1<<20 {
		return
	}
	b.Reset()
	p.p.Put(b)
}

// DefaultPool is a shared buffer pool.
var DefaultPool = NewPool(4096)

// SliceReader reads sequentially from a fixed slice.
type SliceReader struct {
	b   []byte
	off int
}

// NewSliceReader wraps b.
func NewSliceReader(b []byte) *SliceReader {
	return &SliceReader{b: b}
}

// Remaining returns unread bytes.
func (r *SliceReader) Remaining() int {
	return len(r.b) - r.off
}

// Offset returns the current offset.
func (r *SliceReader) Offset() int { return r.off }

// Read implements io.Reader.
func (r *SliceReader) Read(p []byte) (int, error) {
	if r.off >= len(r.b) {
		return 0, io.EOF
	}
	n := copy(p, r.b[r.off:])
	r.off += n
	return n, nil
}

// ReadByte reads one byte.
func (r *SliceReader) ReadByte() (byte, error) {
	if r.off >= len(r.b) {
		return 0, io.EOF
	}
	c := r.b[r.off]
	r.off++
	return c, nil
}

// ReadN reads exactly n bytes or EOF.
func (r *SliceReader) ReadN(n int) ([]byte, error) {
	if n < 0 {
		return nil, ErrOverflow
	}
	if r.off+n > len(r.b) {
		return nil, io.EOF
	}
	out := r.b[r.off : r.off+n]
	r.off += n
	return out, nil
}

// ReadU32LE reads a little-endian uint32.
func (r *SliceReader) ReadU32LE() (uint32, error) {
	b, err := r.ReadN(4)
	if err != nil {
		return 0, err
	}
	return U32LE(b)
}

// ReadU64LE reads a little-endian uint64.
func (r *SliceReader) ReadU64LE() (uint64, error) {
	b, err := r.ReadN(8)
	if err != nil {
		return 0, err
	}
	return U64LE(b)
}

// ReadLP32 reads a length-prefixed blob.
func (r *SliceReader) ReadLP32() ([]byte, error) {
	n, err := r.ReadU32LE()
	if err != nil {
		return nil, err
	}
	return r.ReadN(int(n))
}

// Seek sets the absolute offset.
func (r *SliceReader) Seek(off int) error {
	if off < 0 || off > len(r.b) {
		return ErrOverflow
	}
	r.off = off
	return nil
}

// Ensure import of io in buffer.go — already used above.
var _ = io.EOF
