// Package wal implements a CRC-framed write-ahead log with truncated-tail recovery.
package wal

import (
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/LYH2263/go-prefix-scan/internal/crc32x"
	"github.com/LYH2263/go-prefix-scan/internal/varintx"
)

var (
	ErrClosed     = errors.New("wal: closed")
	ErrCorrupt    = errors.New("wal: corrupt record")
	ErrBadType    = errors.New("wal: bad record type")
	ErrShortWrite = errors.New("wal: short write")
)

// Record types.
const (
	TypePut    byte = 1
	TypeDelete byte = 2
	TypeBatch  byte = 3
)

const (
	headerSize = 4 + 1 + 4 // length(u32 LE) + type + crc32 LE over (type||payload)
	fileName   = "wal.log"
)

// Record is one decoded WAL entry.
type Record struct {
	Type    byte
	Key     []byte
	Value   []byte
	Seq     uint64
	Deleted bool
}

// Options configures WAL open.
type Options struct {
	// SyncEveryN syncs after every N appends; 0 means every append when SyncOnWrite is true.
	SyncEveryN int
	// SyncOnWrite forces Sync after each Append/AppendBatch when SyncEveryN==0.
	SyncOnWrite bool
	// FileName overrides the default wal.log name.
	FileName string
}

func (o *Options) normalize() Options {
	out := Options{SyncOnWrite: true, FileName: fileName}
	if o != nil {
		out = *o
		if out.FileName == "" {
			out.FileName = fileName
		}
	}
	return out
}

// Log is an append-only WAL.
type Log struct {
	mu       sync.Mutex
	dir      string
	path     string
	f        *os.File
	opts     Options
	appends  int
	closed   bool
	size     int64
	nextSeq  uint64
}

// Open opens or creates a WAL in dir.
func Open(dir string, opts *Options) (*Log, error) {
	o := opts.normalize()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, o.FileName)
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	l := &Log{
		dir:  dir,
		path: path,
		f:    f,
		opts: o,
		size: st.Size(),
	}
	if err := l.recoverTail(); err != nil {
		_ = f.Close()
		return nil, err
	}
	return l, nil
}

// recoverTail truncates a torn last frame if the CRC/length is incomplete.
func (l *Log) recoverTail() error {
	if l.size == 0 {
		return nil
	}
	validEnd, maxSeq, err := scanValidPrefix(l.f, l.size)
	if err != nil {
		return err
	}
	l.nextSeq = maxSeq
	if validEnd < l.size {
		if err := l.f.Truncate(validEnd); err != nil {
			return err
		}
		if _, err := l.f.Seek(validEnd, io.SeekStart); err != nil {
			return err
		}
		l.size = validEnd
	} else {
		if _, err := l.f.Seek(0, io.SeekEnd); err != nil {
			return err
		}
	}
	return nil
}

func scanValidPrefix(f *os.File, size int64) (validEnd int64, maxSeq uint64, err error) {
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, 0, err
	}
	var off int64
	buf := make([]byte, headerSize)
	for off+headerSize <= size {
		if _, err := f.ReadAt(buf, off); err != nil {
			return off, maxSeq, nil // treat as torn
		}
		plen := binary.LittleEndian.Uint32(buf[0:4])
		typ := buf[4]
		wantCRC := binary.LittleEndian.Uint32(buf[5:9])
		if typ != TypePut && typ != TypeDelete && typ != TypeBatch {
			return off, maxSeq, nil
		}
		frameEnd := off + headerSize + int64(plen)
		if frameEnd > size {
			return off, maxSeq, nil // truncated payload
		}
		payload := make([]byte, plen)
		if plen > 0 {
			if _, err := f.ReadAt(payload, off+headerSize); err != nil {
				return off, maxSeq, nil
			}
		}
		got := crc32x.FrameChecksum(typ, payload)
		if got != wantCRC {
			return off, maxSeq, nil
		}
		// parse seq from payload for nextSeq tracking
		seq, ok := peekSeq(typ, payload)
		if ok && seq > maxSeq {
			maxSeq = seq
		}
		off = frameEnd
	}
	if off < size {
		// trailing partial header
		return off, maxSeq, nil
	}
	return off, maxSeq, nil
}

func peekSeq(typ byte, payload []byte) (uint64, bool) {
	switch typ {
	case TypePut, TypeDelete:
		seq, _, err := varintx.DecodeUint64(payload)
		if err != nil {
			return 0, false
		}
		return seq, true
	case TypeBatch:
		n, off, err := varintx.DecodeUint64(payload)
		if err != nil {
			return 0, false
		}
		var max uint64
		for i := uint64(0); i < n; i++ {
			seq, noff, err := varintx.DecodeUint64(payload[off:])
			if err != nil {
				return 0, false
			}
			off += noff
			if seq > max {
				max = seq
			}
			// skip type
			if off >= len(payload) {
				return 0, false
			}
			off++
			// skip key
			klen, noff, err := varintx.DecodeUint64(payload[off:])
			if err != nil {
				return 0, false
			}
			off += noff + int(klen)
			// skip value
			vlen, noff, err := varintx.DecodeUint64(payload[off:])
			if err != nil {
				return 0, false
			}
			off += noff + int(vlen)
		}
		return max, true
	default:
		return 0, false
	}
}

// Path returns the WAL file path.
func (l *Log) Path() string { return l.path }

// Size returns the current file size.
func (l *Log) Size() int64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.size
}

// NextSeq returns the next sequence number that would be allocated.
func (l *Log) NextSeq() uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.nextSeq + 1
}

func (l *Log) allocSeq() uint64 {
	l.nextSeq++
	return l.nextSeq
}

// AppendPut writes a put record and returns its sequence number.
func (l *Log) AppendPut(key, value []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, ErrClosed
	}
	seq := l.allocSeq()
	payload := encodeKV(seq, key, value)
	if err := l.writeFrame(TypePut, payload); err != nil {
		return 0, err
	}
	return seq, l.maybeSyncLocked()
}

// AppendDelete writes a delete (tombstone) record.
func (l *Log) AppendDelete(key []byte) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, ErrClosed
	}
	seq := l.allocSeq()
	payload := encodeKV(seq, key, nil)
	if err := l.writeFrame(TypeDelete, payload); err != nil {
		return 0, err
	}
	return seq, l.maybeSyncLocked()
}

// BatchEntry is one entry inside a batch frame.
type BatchEntry struct {
	Key     []byte
	Value   []byte
	Deleted bool
}

// AppendBatch writes multiple entries as one frame; returns the last seq.
func (l *Log) AppendBatch(entries []BatchEntry) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return 0, ErrClosed
	}
	if len(entries) == 0 {
		return l.nextSeq, nil
	}
	var payload []byte
	payload = varintx.EncodeUint64(payload, uint64(len(entries)))
	var last uint64
	for _, e := range entries {
		seq := l.allocSeq()
		last = seq
		payload = varintx.EncodeUint64(payload, seq)
		if e.Deleted {
			payload = append(payload, TypeDelete)
		} else {
			payload = append(payload, TypePut)
		}
		payload = varintx.EncodeUint64(payload, uint64(len(e.Key)))
		payload = append(payload, e.Key...)
		payload = varintx.EncodeUint64(payload, uint64(len(e.Value)))
		payload = append(payload, e.Value...)
	}
	if err := l.writeFrame(TypeBatch, payload); err != nil {
		return 0, err
	}
	return last, l.maybeSyncLocked()
}

func encodeKV(seq uint64, key, value []byte) []byte {
	var b []byte
	b = varintx.EncodeUint64(b, seq)
	b = varintx.EncodeUint64(b, uint64(len(key)))
	b = append(b, key...)
	b = varintx.EncodeUint64(b, uint64(len(value)))
	b = append(b, value...)
	return b
}

func (l *Log) writeFrame(typ byte, payload []byte) error {
	if len(payload) > 1<<28 {
		return fmt.Errorf("wal: payload too large: %d", len(payload))
	}
	crc := crc32x.FrameChecksum(typ, payload)
	hdr := make([]byte, headerSize)
	binary.LittleEndian.PutUint32(hdr[0:4], uint32(len(payload)))
	hdr[4] = typ
	binary.LittleEndian.PutUint32(hdr[5:9], crc)
	n, err := l.f.Write(hdr)
	if err != nil {
		return err
	}
	if n != len(hdr) {
		return ErrShortWrite
	}
	if len(payload) > 0 {
		n, err = l.f.Write(payload)
		if err != nil {
			return err
		}
		if n != len(payload) {
			return ErrShortWrite
		}
	}
	l.size += int64(headerSize + len(payload))
	l.appends++
	return nil
}

func (l *Log) maybeSyncLocked() error {
	if l.opts.SyncEveryN > 0 {
		if l.appends%l.opts.SyncEveryN == 0 {
			return l.f.Sync()
		}
		return nil
	}
	if l.opts.SyncOnWrite {
		return l.f.Sync()
	}
	return nil
}

// Sync flushes the WAL to stable storage.
func (l *Log) Sync() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	return l.f.Sync()
}

// Close syncs and closes the WAL.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	var first error
	if err := l.f.Sync(); err != nil {
		first = err
	}
	if err := l.f.Close(); err != nil && first == nil {
		first = err
	}
	return first
}

// Replay calls fn for each valid record. Truncated tail is ignored.
func (l *Log) Replay(fn func(Record) error) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	size := l.size
	if _, err := l.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	var off int64
	hdr := make([]byte, headerSize)
	for off+headerSize <= size {
		if _, err := l.f.ReadAt(hdr, off); err != nil {
			break
		}
		plen := binary.LittleEndian.Uint32(hdr[0:4])
		typ := hdr[4]
		want := binary.LittleEndian.Uint32(hdr[5:9])
		frameEnd := off + headerSize + int64(plen)
		if frameEnd > size {
			break
		}
		payload := make([]byte, plen)
		if plen > 0 {
			if _, err := l.f.ReadAt(payload, off+headerSize); err != nil {
				break
			}
		}
		if crc32x.FrameChecksum(typ, payload) != want {
			break
		}
		recs, err := decodeFrame(typ, payload)
		if err != nil {
			return err
		}
		for _, r := range recs {
			if err := fn(r); err != nil {
				return err
			}
		}
		off = frameEnd
	}
	// restore file offset to end for appends
	_, err := l.f.Seek(l.size, io.SeekStart)
	return err
}

func decodeFrame(typ byte, payload []byte) ([]Record, error) {
	switch typ {
	case TypePut, TypeDelete:
		r, err := decodeOne(typ, payload)
		if err != nil {
			return nil, err
		}
		return []Record{r}, nil
	case TypeBatch:
		return decodeBatch(payload)
	default:
		return nil, ErrBadType
	}
}

func decodeOne(typ byte, payload []byte) (Record, error) {
	seq, off, err := varintx.DecodeUint64(payload)
	if err != nil {
		return Record{}, ErrCorrupt
	}
	klen, noff, err := varintx.DecodeUint64(payload[off:])
	if err != nil {
		return Record{}, ErrCorrupt
	}
	off += noff
	if off+int(klen) > len(payload) {
		return Record{}, ErrCorrupt
	}
	key := append([]byte(nil), payload[off:off+int(klen)]...)
	off += int(klen)
	vlen, noff, err := varintx.DecodeUint64(payload[off:])
	if err != nil {
		return Record{}, ErrCorrupt
	}
	off += noff
	if off+int(vlen) > len(payload) {
		return Record{}, ErrCorrupt
	}
	val := append([]byte(nil), payload[off:off+int(vlen)]...)
	return Record{
		Type:    typ,
		Key:     key,
		Value:   val,
		Seq:     seq,
		Deleted: typ == TypeDelete,
	}, nil
}

func decodeBatch(payload []byte) ([]Record, error) {
	n, off, err := varintx.DecodeUint64(payload)
	if err != nil {
		return nil, ErrCorrupt
	}
	out := make([]Record, 0, n)
	for i := uint64(0); i < n; i++ {
		seq, noff, err := varintx.DecodeUint64(payload[off:])
		if err != nil {
			return nil, ErrCorrupt
		}
		off += noff
		if off >= len(payload) {
			return nil, ErrCorrupt
		}
		typ := payload[off]
		off++
		klen, noff, err := varintx.DecodeUint64(payload[off:])
		if err != nil {
			return nil, ErrCorrupt
		}
		off += noff
		if off+int(klen) > len(payload) {
			return nil, ErrCorrupt
		}
		key := append([]byte(nil), payload[off:off+int(klen)]...)
		off += int(klen)
		vlen, noff, err := varintx.DecodeUint64(payload[off:])
		if err != nil {
			return nil, ErrCorrupt
		}
		off += noff
		if off+int(vlen) > len(payload) {
			return nil, ErrCorrupt
		}
		val := append([]byte(nil), payload[off:off+int(vlen)]...)
		off += int(vlen)
		out = append(out, Record{
			Type:    typ,
			Key:     key,
			Value:   val,
			Seq:     seq,
			Deleted: typ == TypeDelete,
		})
	}
	return out, nil
}

// Reset truncates the WAL to empty (used after flush).
func (l *Log) Reset() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return ErrClosed
	}
	if err := l.f.Truncate(0); err != nil {
		return err
	}
	if _, err := l.f.Seek(0, io.SeekStart); err != nil {
		return err
	}
	l.size = 0
	l.appends = 0
	// keep nextSeq monotonic across resets
	return l.f.Sync()
}

// SetNextSeq forces the sequence counter (used when recovering from SSTables).
func (l *Log) SetNextSeq(seq uint64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if seq > l.nextSeq {
		l.nextSeq = seq
	}
}

// CRCTable exposes the table used for frames (for tests).
func CRCTable() *crc32.Table {
	return crc32.MakeTable(crc32.Castagnoli)
}
