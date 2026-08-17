// Package sstable implements block-based SSTables with restart points and bloom filters.
package sstable

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"

	"github.com/LYH2263/go-prefix-scan/internal/bloomx"
	"github.com/LYH2263/go-prefix-scan/internal/byteutil"
	"github.com/LYH2263/go-prefix-scan/internal/crc32x"
	"github.com/LYH2263/go-prefix-scan/internal/varintx"
)

var (
	ErrCorrupt     = errors.New("sstable: corrupt file")
	ErrClosed      = errors.New("sstable: closed")
	ErrNotFound    = errors.New("sstable: not found")
	ErrInvalid     = errors.New("sstable: invalid argument")
	ErrBadMagic    = errors.New("sstable: bad magic")
)

const (
	magicFooter      = uint32(0x53535442) // SSTB
	restartInterval  = 16
	defaultBlockSize = 4096
	flagDelete       = byte(1)
	flagValue        = byte(0)
)

// Entry is one logical KV written to an SSTable.
type Entry struct {
	Key     []byte
	Value   []byte
	Seq     uint64
	Deleted bool
}

// Writer builds an SSTable file.
type Writer struct {
	f           *os.File
	path        string
	blockSize   int
	restartInt  int
	buf         []byte
	restarts    []uint32
	entryCount  int
	blockEntries int
	lastKey     []byte
	index       []indexRec
	bloom       *bloomx.Builder
	minKey      []byte
	maxKey      []byte
	maxSeq      uint64
	closed      bool
}

type indexRec struct {
	lastKey []byte
	offset  uint64
	length  uint64
}

// WriterOptions configures SSTable writing.
type WriterOptions struct {
	BlockSize       int
	RestartInterval int
	BloomFP         float64
}

func (o *WriterOptions) normalize() WriterOptions {
	out := WriterOptions{BlockSize: defaultBlockSize, RestartInterval: restartInterval, BloomFP: 0.01}
	if o != nil {
		if o.BlockSize > 0 {
			out.BlockSize = o.BlockSize
		}
		if o.RestartInterval > 0 {
			out.RestartInterval = o.RestartInterval
		}
		if o.BloomFP > 0 {
			out.BloomFP = o.BloomFP
		}
	}
	return out
}

// Create opens a new SSTable for writing at path.
func Create(path string, opts *WriterOptions) (*Writer, error) {
	o := opts.normalize()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	return &Writer{
		f:          f,
		path:       path,
		blockSize:  o.BlockSize,
		restartInt: o.RestartInterval,
		bloom:      bloomx.NewBuilder(o.BloomFP),
		buf:        make([]byte, 0, o.BlockSize),
	}, nil
}

// Add appends an entry. Entries must be added in ascending key order.
// Tombstones (Deleted=true) ARE written — required for correct LSM semantics.
func (w *Writer) Add(e Entry) error {
	if w.closed {
		return ErrClosed
	}
	if e.Key == nil {
		return ErrInvalid
	}
	// NOTE: tombstones must be persisted. Do not skip Deleted entries.
	if w.lastKey != nil && byteutil.Compare(e.Key, w.lastKey) < 0 {
		return fmt.Errorf("sstable: keys out of order: %q < %q", e.Key, w.lastKey)
	}
	if w.minKey == nil {
		w.minKey = byteutil.Clone(e.Key)
	}
	w.maxKey = byteutil.Clone(e.Key)
	if e.Seq > w.maxSeq {
		w.maxSeq = e.Seq
	}
	w.bloom.Add(e.Key)

	shared := 0
	if w.blockEntries%w.restartInt != 0 && w.lastKey != nil {
		shared = byteutil.SharedPrefixLen(w.lastKey, e.Key)
	} else {
		w.restarts = append(w.restarts, uint32(len(w.buf)))
	}
	nonShared := len(e.Key) - shared
	var enc []byte
	enc = varintx.EncodeUint64(enc, uint64(shared))
	enc = varintx.EncodeUint64(enc, uint64(nonShared))
	enc = varintx.EncodeUint64(enc, uint64(len(e.Value)))
	enc = varintx.EncodeUint64(enc, e.Seq)
	flag := flagValue
	if e.Deleted {
		flag = flagDelete
	}
	enc = append(enc, flag)
	enc = append(enc, e.Key[shared:]...)
	enc = append(enc, e.Value...)

	w.buf = append(w.buf, enc...)
	w.lastKey = byteutil.Clone(e.Key)
	w.entryCount++
	w.blockEntries++

	if len(w.buf) >= w.blockSize {
		return w.flushBlock()
	}
	return nil
}

func (w *Writer) flushBlock() error {
	if len(w.buf) == 0 {
		return nil
	}
	// trailer: restart array + count + crc
	block := append([]byte(nil), w.buf...)
	for _, off := range w.restarts {
		var tmp [4]byte
		binary.LittleEndian.PutUint32(tmp[:], off)
		block = append(block, tmp[:]...)
	}
	var tmp [4]byte
	binary.LittleEndian.PutUint32(tmp[:], uint32(len(w.restarts)))
	block = append(block, tmp[:]...)
	crc := crc32x.ChecksumCastagnoli(block)
	binary.LittleEndian.PutUint32(tmp[:], crc)
	block = append(block, tmp[:]...)

	off, err := w.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}
	n, err := w.f.Write(block)
	if err != nil {
		return err
	}
	if n != len(block) {
		return io.ErrShortWrite
	}
	w.index = append(w.index, indexRec{
		lastKey: byteutil.Clone(w.lastKey),
		offset:  uint64(off),
		length:  uint64(len(block)),
	})
	w.buf = w.buf[:0]
	w.restarts = w.restarts[:0]
	w.blockEntries = 0
	return nil
}

// Finish writes remaining data, index, bloom, and footer; closes the file.
func (w *Writer) Finish() (*Meta, error) {
	if w.closed {
		return nil, ErrClosed
	}
	if err := w.flushBlock(); err != nil {
		return nil, err
	}
	bloomOff, err := w.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	bf := w.bloom.Build()
	bdata, err := bf.MarshalBinary()
	if err != nil {
		return nil, err
	}
	if _, err := w.f.Write(bdata); err != nil {
		return nil, err
	}
	bloomLen := int64(len(bdata))

	indexOff, err := w.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}
	var indexBuf []byte
	indexBuf = varintx.EncodeUint64(indexBuf, uint64(len(w.index)))
	for _, rec := range w.index {
		indexBuf = varintx.EncodeUint64(indexBuf, uint64(len(rec.lastKey)))
		indexBuf = append(indexBuf, rec.lastKey...)
		indexBuf = varintx.EncodeUint64(indexBuf, rec.offset)
		indexBuf = varintx.EncodeUint64(indexBuf, rec.length)
	}
	indexCRC := crc32x.ChecksumCastagnoli(indexBuf)
	var crcBuf [4]byte
	binary.LittleEndian.PutUint32(crcBuf[:], indexCRC)
	indexBuf = append(indexBuf, crcBuf[:]...)
	if _, err := w.f.Write(indexBuf); err != nil {
		return nil, err
	}
	indexLen := int64(len(indexBuf))

	// footer: magic(4) | indexOff(8) | indexLen(8) | bloomOff(8) | bloomLen(8) | maxSeq(8) | entries(8) | magic(4)
	const footerN = 56
	var footer [footerN]byte
	binary.LittleEndian.PutUint32(footer[0:4], magicFooter)
	binary.LittleEndian.PutUint64(footer[4:12], uint64(indexOff))
	binary.LittleEndian.PutUint64(footer[12:20], uint64(indexLen))
	binary.LittleEndian.PutUint64(footer[20:28], uint64(bloomOff))
	binary.LittleEndian.PutUint64(footer[28:36], uint64(bloomLen))
	binary.LittleEndian.PutUint64(footer[36:44], w.maxSeq)
	binary.LittleEndian.PutUint64(footer[44:52], uint64(w.entryCount))
	binary.LittleEndian.PutUint32(footer[52:56], magicFooter)
	if _, err := w.f.Write(footer[:]); err != nil {
		return nil, err
	}
	if err := w.f.Sync(); err != nil {
		return nil, err
	}
	st, err := w.f.Stat()
	if err != nil {
		return nil, err
	}
	if err := w.f.Close(); err != nil {
		return nil, err
	}
	w.closed = true
	return &Meta{
		Path:     w.path,
		MinKey:   w.minKey,
		MaxKey:   w.maxKey,
		MaxSeq:   w.maxSeq,
		Entries:  uint64(w.entryCount),
		FileSize: st.Size(),
	}, nil
}

// Abort closes and removes the partial file.
func (w *Writer) Abort() error {
	if w.closed {
		return nil
	}
	w.closed = true
	_ = w.f.Close()
	return os.Remove(w.path)
}

// Meta describes a finished SSTable.
type Meta struct {
	Path     string
	MinKey   []byte
	MaxKey   []byte
	MaxSeq   uint64
	Entries  uint64
	FileSize int64
}

// Reader reads an SSTable.
type Reader struct {
	f        *os.File
	path     string
	index    []indexRec
	bloom    *bloomx.Filter
	maxSeq   uint64
	entries  uint64
	minKey   []byte
	maxKey   []byte
	fileSize int64
}

// Open opens an SSTable for reading.
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	if st.Size() < 56 {
		_ = f.Close()
		return nil, ErrCorrupt
	}
	var footer [56]byte
	if _, err := f.ReadAt(footer[:], st.Size()-56); err != nil {
		_ = f.Close()
		return nil, err
	}
	if binary.LittleEndian.Uint32(footer[0:4]) != magicFooter ||
		binary.LittleEndian.Uint32(footer[52:56]) != magicFooter {
		_ = f.Close()
		return nil, ErrBadMagic
	}
	indexOff := int64(binary.LittleEndian.Uint64(footer[4:12]))
	indexLen := int64(binary.LittleEndian.Uint64(footer[12:20]))
	bloomOff := int64(binary.LittleEndian.Uint64(footer[20:28]))
	bloomLen := int64(binary.LittleEndian.Uint64(footer[28:36]))
	maxSeq := binary.LittleEndian.Uint64(footer[36:44])
	entries := binary.LittleEndian.Uint64(footer[44:52])

	bloomData := make([]byte, bloomLen)
	if bloomLen > 0 {
		if _, err := f.ReadAt(bloomData, bloomOff); err != nil {
			_ = f.Close()
			return nil, err
		}
	}
	bf, err := bloomx.UnmarshalBinary(bloomData)
	if err != nil {
		_ = f.Close()
		return nil, err
	}
	indexData := make([]byte, indexLen)
	if _, err := f.ReadAt(indexData, indexOff); err != nil {
		_ = f.Close()
		return nil, err
	}
	if len(indexData) < 4 {
		_ = f.Close()
		return nil, ErrCorrupt
	}
	body := indexData[:len(indexData)-4]
	want := binary.LittleEndian.Uint32(indexData[len(indexData)-4:])
	if crc32x.ChecksumCastagnoli(body) != want {
		_ = f.Close()
		return nil, ErrCorrupt
	}
	n, off, err := varintx.DecodeUint64(body)
	if err != nil {
		_ = f.Close()
		return nil, ErrCorrupt
	}
	index := make([]indexRec, 0, n)
	for i := uint64(0); i < n; i++ {
		klen, noff, err := varintx.DecodeUint64(body[off:])
		if err != nil {
			_ = f.Close()
			return nil, ErrCorrupt
		}
		off += noff
		if off+int(klen) > len(body) {
			_ = f.Close()
			return nil, ErrCorrupt
		}
		key := byteutil.Clone(body[off : off+int(klen)])
		off += int(klen)
		boff, noff, err := varintx.DecodeUint64(body[off:])
		if err != nil {
			_ = f.Close()
			return nil, ErrCorrupt
		}
		off += noff
		blen, noff, err := varintx.DecodeUint64(body[off:])
		if err != nil {
			_ = f.Close()
			return nil, ErrCorrupt
		}
		off += noff
		index = append(index, indexRec{lastKey: key, offset: boff, length: blen})
	}
	r := &Reader{
		f:        f,
		path:     path,
		index:    index,
		bloom:    bf,
		maxSeq:   maxSeq,
		entries:  entries,
		fileSize: st.Size(),
	}
	if len(index) > 0 {
		r.maxKey = byteutil.Clone(index[len(index)-1].lastKey)
		// min key approximated by first key of first block — load lazily on demand
	}
	return r, nil
}

// Path returns the file path.
func (r *Reader) Path() string { return r.path }

// MaxSeq returns the max sequence in the table.
func (r *Reader) MaxSeq() uint64 { return r.maxSeq }

// Entries returns entry count.
func (r *Reader) Entries() uint64 { return r.entries }

// FileSize returns file size.
func (r *Reader) FileSize() int64 { return r.fileSize }

// MayContain uses the bloom filter.
func (r *Reader) MayContain(key []byte) bool {
	return r.bloom.MayContain(key)
}

// Close closes the file.
func (r *Reader) Close() error {
	if r.f == nil {
		return nil
	}
	err := r.f.Close()
	r.f = nil
	return err
}

func (r *Reader) readBlock(rec indexRec) ([]byte, error) {
	buf := make([]byte, rec.length)
	if _, err := r.f.ReadAt(buf, int64(rec.offset)); err != nil {
		return nil, err
	}
	if len(buf) < 8 {
		return nil, ErrCorrupt
	}
	body := buf[:len(buf)-4]
	want := binary.LittleEndian.Uint32(buf[len(buf)-4:])
	if crc32x.ChecksumCastagnoli(body) != want {
		return nil, ErrCorrupt
	}
	return body, nil
}

type blockIter struct {
	data     []byte
	restarts []uint32
	off      int
	key      []byte
	value    []byte
	seq      uint64
	deleted  bool
	valid    bool
}

func parseRestarts(block []byte) (data []byte, restarts []uint32, err error) {
	if len(block) < 4 {
		return nil, nil, ErrCorrupt
	}
	n := binary.LittleEndian.Uint32(block[len(block)-4:])
	need := int(n)*4 + 4
	if len(block) < need {
		return nil, nil, ErrCorrupt
	}
	restartOff := len(block) - need
	data = block[:restartOff]
	restarts = make([]uint32, n)
	for i := 0; i < int(n); i++ {
		restarts[i] = binary.LittleEndian.Uint32(block[restartOff+i*4 : restartOff+i*4+4])
	}
	return data, restarts, nil
}

func newBlockIter(block []byte) (*blockIter, error) {
	data, restarts, err := parseRestarts(block)
	if err != nil {
		return nil, err
	}
	it := &blockIter{data: data, restarts: restarts}
	it.Next()
	return it, nil
}

func (it *blockIter) Next() {
	if it.off >= len(it.data) {
		it.valid = false
		return
	}
	shared, noff, err := varintx.DecodeUint64(it.data[it.off:])
	if err != nil {
		it.valid = false
		return
	}
	it.off += noff
	nonShared, noff, err := varintx.DecodeUint64(it.data[it.off:])
	if err != nil {
		it.valid = false
		return
	}
	it.off += noff
	vlen, noff, err := varintx.DecodeUint64(it.data[it.off:])
	if err != nil {
		it.valid = false
		return
	}
	it.off += noff
	seq, noff, err := varintx.DecodeUint64(it.data[it.off:])
	if err != nil {
		it.valid = false
		return
	}
	it.off += noff
	if it.off >= len(it.data) {
		it.valid = false
		return
	}
	flag := it.data[it.off]
	it.off++
	if it.off+int(nonShared)+int(vlen) > len(it.data) {
		it.valid = false
		return
	}
	key := make([]byte, int(shared)+int(nonShared))
	copy(key, it.key[:shared])
	copy(key[shared:], it.data[it.off:it.off+int(nonShared)])
	it.off += int(nonShared)
	val := byteutil.Clone(it.data[it.off : it.off+int(vlen)])
	it.off += int(vlen)
	it.key = key
	it.value = val
	it.seq = seq
	it.deleted = flag == flagDelete
	it.valid = true
}

func (it *blockIter) Valid() bool { return it.valid }

func (it *blockIter) Seek(target []byte) {
	// binary search restarts
	n := len(it.restarts)
	if n == 0 {
		it.off = 0
		it.key = nil
		it.Next()
		for it.valid && byteutil.Compare(it.key, target) < 0 {
			it.Next()
		}
		return
	}
	lo, hi := 0, n-1
	for lo < hi {
		mid := (lo + hi + 1) / 2
		it.off = int(it.restarts[mid])
		it.key = nil
		it.Next()
		if !it.valid {
			hi = mid - 1
			continue
		}
		if byteutil.Compare(it.key, target) <= 0 {
			lo = mid
		} else {
			hi = mid - 1
		}
	}
	it.off = int(it.restarts[lo])
	it.key = nil
	it.Next()
	for it.valid && byteutil.Compare(it.key, target) < 0 {
		it.Next()
	}
}

// Get returns the newest entry for key in this table, or false if absent.
func (r *Reader) Get(key []byte) (Entry, bool, error) {
	if !r.MayContain(key) {
		return Entry{}, false, nil
	}
	if len(r.index) == 0 {
		return Entry{}, false, nil
	}
	// find first block whose lastKey >= key
	idx := sort.Search(len(r.index), func(i int) bool {
		return byteutil.Compare(r.index[i].lastKey, key) >= 0
	})
	if idx >= len(r.index) {
		return Entry{}, false, nil
	}
	block, err := r.readBlock(r.index[idx])
	if err != nil {
		return Entry{}, false, err
	}
	it, err := newBlockIter(block)
	if err != nil {
		return Entry{}, false, err
	}
	it.Seek(key)
	if it.Valid() && byteutil.Equal(it.key, key) {
		return Entry{Key: byteutil.Clone(it.key), Value: byteutil.Clone(it.value), Seq: it.seq, Deleted: it.deleted}, true, nil
	}
	return Entry{}, false, nil
}

// Iterator iterates all entries in key order.
type Iterator struct {
	r       *Reader
	bi      int
	blockIt *blockIter
	err     error
}

// NewIterator returns an iterator at the first key.
func (r *Reader) NewIterator() *Iterator {
	it := &Iterator{r: r, bi: -1}
	it.Next()
	return it
}

// Valid reports whether positioned on an entry.
func (it *Iterator) Valid() bool {
	return it.err == nil && it.blockIt != nil && it.blockIt.Valid()
}

// Next advances.
func (it *Iterator) Next() {
	if it.err != nil {
		return
	}
	if it.blockIt != nil {
		it.blockIt.Next()
		if it.blockIt.Valid() {
			return
		}
	}
	it.bi++
	for it.bi < len(it.r.index) {
		block, err := it.r.readBlock(it.r.index[it.bi])
		if err != nil {
			it.err = err
			return
		}
		bit, err := newBlockIter(block)
		if err != nil {
			it.err = err
			return
		}
		it.blockIt = bit
		if bit.Valid() {
			return
		}
		it.bi++
	}
	it.blockIt = nil
}

// Seek positions at first key >= target.
func (it *Iterator) Seek(target []byte) {
	it.err = nil
	if len(it.r.index) == 0 {
		it.blockIt = nil
		return
	}
	idx := sort.Search(len(it.r.index), func(i int) bool {
		return byteutil.Compare(it.r.index[i].lastKey, target) >= 0
	})
	if idx >= len(it.r.index) {
		it.bi = len(it.r.index)
		it.blockIt = nil
		return
	}
	it.bi = idx
	block, err := it.r.readBlock(it.r.index[idx])
	if err != nil {
		it.err = err
		return
	}
	bit, err := newBlockIter(block)
	if err != nil {
		it.err = err
		return
	}
	bit.Seek(target)
	it.blockIt = bit
	if !bit.Valid() {
		it.Next()
	}
}

// Key returns current key.
func (it *Iterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return byteutil.Clone(it.blockIt.key)
}

// Value returns current value.
func (it *Iterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return byteutil.Clone(it.blockIt.value)
}

// Seq returns current seq.
func (it *Iterator) Seq() uint64 {
	if !it.Valid() {
		return 0
	}
	return it.blockIt.seq
}

// Deleted reports tombstone.
func (it *Iterator) Deleted() bool {
	if !it.Valid() {
		return false
	}
	return it.blockIt.deleted
}

// Err returns iteration error.
func (it *Iterator) Err() error { return it.err }

// Entry returns current entry.
func (it *Iterator) Entry() Entry {
	if !it.Valid() {
		return Entry{}
	}
	return Entry{
		Key:     it.Key(),
		Value:   it.Value(),
		Seq:     it.Seq(),
		Deleted: it.Deleted(),
	}
}

// WriteFile writes entries (must be sorted by key) to path.
func WriteFile(path string, entries []Entry, opts *WriterOptions) (*Meta, error) {
	w, err := Create(path, opts)
	if err != nil {
		return nil, err
	}
	for _, e := range entries {
		if err := w.Add(e); err != nil {
			_ = w.Abort()
			return nil, err
		}
	}
	return w.Finish()
}
