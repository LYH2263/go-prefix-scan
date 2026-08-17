// Package lsmkv is a durable log-structured merge-tree key-value library.
package lsmkv

import (
	"bytes"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/LYH2263/go-prefix-scan/internal/byteutil"
	"github.com/LYH2263/go-prefix-scan/internal/compact"
	"github.com/LYH2263/go-prefix-scan/internal/filelock"
	"github.com/LYH2263/go-prefix-scan/internal/manifest"
	"github.com/LYH2263/go-prefix-scan/internal/memtable"
	"github.com/LYH2263/go-prefix-scan/internal/sstable"
	"github.com/LYH2263/go-prefix-scan/internal/wal"
)

var (
	ErrClosed    = errors.New("lsmkv: closed")
	ErrNotFound  = errors.New("lsmkv: not found")
	ErrInvalid   = errors.New("lsmkv: invalid argument")
	ErrReadOnly  = errors.New("lsmkv: read only")
)

// SyncPolicy controls when the WAL is fsynced.
type SyncPolicy int

const (
	// SyncFull fsyncs the WAL after every write.
	SyncFull SyncPolicy = iota
	// SyncBatch fsyncs every N writes (see Options.SyncEveryN).
	SyncBatch
	// SyncNone never fsyncs automatically (caller must Sync).
	SyncNone
)

// Options configures database open behavior.
type Options struct {
	// MemtableBytes is the soft flush threshold (default 4MiB).
	MemtableBytes int64
	// SyncPolicy selects WAL durability.
	SyncPolicy SyncPolicy
	// SyncEveryN is used when SyncPolicy == SyncBatch (default 32).
	SyncEveryN int
	// CompactThreshold is the minimum number of L0 SSTs to trigger Compact (default 2).
	CompactThreshold int
	// ReadOnly opens without taking a write lock mutation path (still locks dir).
	ReadOnly bool
}

func (o *Options) normalize() Options {
	out := Options{
		MemtableBytes:    4 << 20,
		SyncPolicy:       SyncFull,
		SyncEveryN:       32,
		CompactThreshold: 2,
	}
	if o == nil {
		return out
	}
	if o.MemtableBytes > 0 {
		out.MemtableBytes = o.MemtableBytes
	}
	out.SyncPolicy = o.SyncPolicy
	if o.SyncEveryN > 0 {
		out.SyncEveryN = o.SyncEveryN
	}
	if o.CompactThreshold > 0 {
		out.CompactThreshold = o.CompactThreshold
	}
	out.ReadOnly = o.ReadOnly
	return out
}

func (o Options) walOpts() *wal.Options {
	switch o.SyncPolicy {
	case SyncNone:
		return &wal.Options{SyncOnWrite: false, SyncEveryN: 0}
	case SyncBatch:
		return &wal.Options{SyncOnWrite: false, SyncEveryN: o.SyncEveryN}
	default:
		return &wal.Options{SyncOnWrite: true, SyncEveryN: 0}
	}
}

// DB is an LSM key-value store rooted at a directory.
type DB struct {
	mu       sync.RWMutex
	dir      string
	opts     Options
	lock     *filelock.Lock
	wal      *wal.Log
	mem      *memtable.MemTable
	manif    *manifest.Manifest
	tables   []*sstable.Reader
	closed   bool
}

// Open opens or creates a database in dir.
func Open(dir string, opts *Options) (*DB, error) {
	o := opts.normalize()
	if dir == "" {
		return nil, ErrInvalid
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	lk, err := filelock.Acquire(dir, nil)
	if err != nil {
		return nil, err
	}
	mf, err := manifest.Open(dir)
	if err != nil {
		_ = lk.Release()
		return nil, err
	}
	w, err := wal.Open(dir, o.walOpts())
	if err != nil {
		_ = lk.Release()
		return nil, err
	}
	// sequence continuity: max(manifest files, wal recovered)
	maxSeq := mf.MaxSeqAmongFiles()
	if mf.LogSeq() > maxSeq {
		maxSeq = mf.LogSeq()
	}
	w.SetNextSeq(maxSeq)

	db := &DB{
		dir:   dir,
		opts:  o,
		lock:  lk,
		wal:   w,
		mem:   memtable.NewMemTable(o.MemtableBytes),
		manif: mf,
	}
	if err := db.openTables(); err != nil {
		_ = w.Close()
		_ = lk.Release()
		return nil, err
	}
	if err := db.replayWAL(); err != nil {
		db.closeUnlocked()
		return nil, err
	}
	return db, nil
}

func (db *DB) openTables() error {
	files := db.manif.Files()
	tables := make([]*sstable.Reader, 0, len(files))
	for _, f := range files {
		path := filepath.Join(db.dir, f.Name)
		r, err := sstable.Open(path)
		if err != nil {
			for _, t := range tables {
				_ = t.Close()
			}
			return err
		}
		tables = append(tables, r)
	}
	db.tables = tables
	return nil
}

func (db *DB) replayWAL() error {
	return db.wal.Replay(func(rec wal.Record) error {
		if rec.Deleted {
			db.mem.Delete(rec.Key, rec.Seq)
		} else {
			db.mem.Put(rec.Key, rec.Value, rec.Seq)
		}
		return nil
	})
}

// Close flushes memtable, syncs, and releases resources.
func (db *DB) Close() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.closeUnlocked()
}

func (db *DB) closeUnlocked() error {
	if db.closed {
		return nil
	}
	db.closed = true
	var first error
	if !db.opts.ReadOnly && db.mem.Len() > 0 {
		if err := db.flushLocked(); err != nil && first == nil {
			first = err
		}
	}
	if db.wal != nil {
		if err := db.wal.Close(); err != nil && first == nil {
			first = err
		}
	}
	for _, t := range db.tables {
		if err := t.Close(); err != nil && first == nil {
			first = err
		}
	}
	if db.lock != nil {
		if err := db.lock.Release(); err != nil && first == nil {
			first = err
		}
	}
	return first
}

// Dir returns the database directory.
func (db *DB) Dir() string { return db.dir }

// Put writes a key/value.
func (db *DB) Put(key, value []byte) error {
	if len(key) == 0 {
		return ErrInvalid
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.opts.ReadOnly {
		return ErrReadOnly
	}
	seq, err := db.wal.AppendPut(key, value)
	if err != nil {
		return err
	}
	db.mem.Put(key, value, seq)
	if db.mem.ShouldFlush() {
		return db.flushLocked()
	}
	return nil
}

// Delete writes a tombstone for key.
func (db *DB) Delete(key []byte) error {
	if len(key) == 0 {
		return ErrInvalid
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.opts.ReadOnly {
		return ErrReadOnly
	}
	seq, err := db.wal.AppendDelete(key)
	if err != nil {
		return err
	}
	db.mem.Delete(key, seq)
	if db.mem.ShouldFlush() {
		return db.flushLocked()
	}
	return nil
}

// Get returns the value for key or ErrNotFound.
func (db *DB) Get(key []byte) ([]byte, error) {
	db.mu.RLock()
	defer db.mu.RUnlock()
	if db.closed {
		return nil, ErrClosed
	}
	if e, ok := db.mem.Get(key); ok {
		if e.Deleted {
			return nil, ErrNotFound
		}
		return byteutil.Clone(e.Value), nil
	}
	var (
		best    sstable.Entry
		found   bool
	)
	// Scan newer tables first (append order = older first; reverse).
	for i := len(db.tables) - 1; i >= 0; i-- {
		t := db.tables[i]
		e, ok, err := t.Get(key)
		if err != nil {
			return nil, err
		}
		if !ok {
			continue
		}
		if !found || e.Seq > best.Seq {
			best = e
			found = true
		}
	}
	if !found {
		return nil, ErrNotFound
	}
	if best.Deleted {
		return nil, ErrNotFound
	}
	return byteutil.Clone(best.Value), nil
}

// WriteBatch applies multiple mutations atomically in one WAL frame.
type WriteBatch struct {
	entries []wal.BatchEntry
}

// NewWriteBatch creates an empty batch.
func NewWriteBatch() *WriteBatch { return &WriteBatch{} }

// Put queues a put.
func (b *WriteBatch) Put(key, value []byte) {
	b.entries = append(b.entries, wal.BatchEntry{
		Key: byteutil.Clone(key), Value: byteutil.Clone(value),
	})
}

// Delete queues a delete.
func (b *WriteBatch) Delete(key []byte) {
	b.entries = append(b.entries, wal.BatchEntry{
		Key: byteutil.Clone(key), Deleted: true,
	})
}

// Len returns queued entries.
func (b *WriteBatch) Len() int { return len(b.entries) }

// Clear resets the batch.
func (b *WriteBatch) Clear() { b.entries = b.entries[:0] }

// Write applies the batch.
func (db *DB) Write(batch *WriteBatch) error {
	if batch == nil || len(batch.entries) == 0 {
		return nil
	}
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.opts.ReadOnly {
		return ErrReadOnly
	}
	_, err := db.wal.AppendBatch(batch.entries)
	if err != nil {
		return err
	}
	// Re-read seqs by applying in order with Get from wal nextSeq — AppendBatch
	// already allocated seqs internally; replay into mem by scanning entries with
	// synthetic increasing seqs matching WAL. We reconstruct by asking wal NextSeq
	// after append: last seq = NextSeq()-1, first = last-len+1.
	last := db.wal.NextSeq() - 1
	first := last - uint64(len(batch.entries)) + 1
	for i, e := range batch.entries {
		seq := first + uint64(i)
		if e.Deleted {
			db.mem.Delete(e.Key, seq)
		} else {
			db.mem.Put(e.Key, e.Value, seq)
		}
	}
	if db.mem.ShouldFlush() {
		return db.flushLocked()
	}
	return nil
}

// Sync fsyncs the WAL.
func (db *DB) Sync() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	return db.wal.Sync()
}

// Flush forces memtable to an SSTable.
func (db *DB) Flush() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.opts.ReadOnly {
		return ErrReadOnly
	}
	return db.flushLocked()
}

func (db *DB) flushLocked() error {
	snap := db.mem.Snapshot()
	if len(snap) == 0 {
		return nil
	}
	name, err := db.manif.AllocFileName()
	if err != nil {
		return err
	}
	path := filepath.Join(db.dir, name)
	entries := make([]sstable.Entry, 0, len(snap))
	var maxSeq uint64
	var minKey, maxKey []byte
	for _, e := range snap {
		entries = append(entries, sstable.Entry{
			Key: e.Key, Value: e.Value, Seq: e.Seq, Deleted: e.Deleted,
		})
		if e.Seq > maxSeq {
			maxSeq = e.Seq
		}
		if minKey == nil || byteutil.Compare(e.Key, minKey) < 0 {
			minKey = e.Key
		}
		if maxKey == nil || byteutil.Compare(e.Key, maxKey) > 0 {
			maxKey = e.Key
		}
	}
	meta, err := sstable.WriteFile(path, entries, nil)
	if err != nil {
		return err
	}
	fm := manifest.FileMeta{
		Name: name, Level: 0,
		MinKey: byteutil.Clone(minKey), MaxKey: byteutil.Clone(maxKey),
		MaxSeq: maxSeq, Entries: uint64(len(entries)), FileSize: meta.FileSize,
	}
	if err := db.manif.AddFile(fm, maxSeq); err != nil {
		_ = os.Remove(path)
		return err
	}
	r, err := sstable.Open(path)
	if err != nil {
		return err
	}
	db.tables = append(db.tables, r)
	db.mem.Clear()
	if err := db.wal.Reset(); err != nil {
		return err
	}
	db.wal.SetNextSeq(maxSeq)
	return nil
}

// Compact merges SSTables to reduce file count; tombstones are preserved.
func (db *DB) Compact() error {
	db.mu.Lock()
	defer db.mu.Unlock()
	if db.closed {
		return ErrClosed
	}
	if db.opts.ReadOnly {
		return ErrReadOnly
	}
	if len(db.tables) < db.opts.CompactThreshold {
		return nil
	}
	refs := make([]compact.FileRef, 0, len(db.tables))
	files := db.manif.Files()
	nameByPath := map[string]manifest.FileMeta{}
	for _, f := range files {
		nameByPath[filepath.Join(db.dir, f.Name)] = f
	}
	for _, t := range db.tables {
		fm := nameByPath[t.Path()]
		refs = append(refs, compact.FileRef{
			Path: t.Path(), Name: fm.Name, Level: fm.Level, Size: t.FileSize(),
		})
	}
	alloc := func() (string, error) { return db.manif.AllocFileName() }
	res, err := compact.CompactDir(db.dir, refs, alloc, &compact.Options{DropTombstones: false})
	if err != nil {
		if errors.Is(err, compact.ErrNoInputs) {
			return nil
		}
		return err
	}
	// Close and remove old tables that were merged
	removeSet := map[string]struct{}{}
	for _, p := range res.Removed {
		removeSet[p] = struct{}{}
	}
	kept := make([]*sstable.Reader, 0, len(db.tables))
	var removeNames []string
	for _, t := range db.tables {
		if _, ok := removeSet[t.Path()]; ok {
			removeNames = append(removeNames, filepath.Base(t.Path()))
			_ = t.Close()
			continue
		}
		kept = append(kept, t)
	}
	outReader, err := sstable.Open(res.OutputPath)
	if err != nil {
		return err
	}
	kept = append(kept, outReader)
	add := []manifest.FileMeta{{
		Name: filepath.Base(res.OutputPath), Level: 1,
		MinKey: byteutil.Clone(res.Meta.MinKey), MaxKey: byteutil.Clone(res.Meta.MaxKey),
		MaxSeq: res.Meta.MaxSeq, Entries: res.Meta.Entries, FileSize: res.Meta.FileSize,
	}}
	if err := db.manif.Replace(removeNames, add, res.Meta.MaxSeq); err != nil {
		return err
	}
	for p := range removeSet {
		_ = os.Remove(p)
	}
	db.tables = kept
	return nil
}

// Stats returns basic database statistics.
type Stats struct {
	MemtableBytes int64
	MemtableKeys  int
	SSTFiles      int
	WALSize       int64
}

// Stats returns current stats.
func (db *DB) Stats() Stats {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return Stats{
		MemtableBytes: db.mem.Bytes(),
		MemtableKeys:  db.mem.Len(),
		SSTFiles:      len(db.tables),
		WALSize:       db.wal.Size(),
	}
}

// TableCount returns the number of open SSTables.
func (db *DB) TableCount() int {
	db.mu.RLock()
	defer db.mu.RUnlock()
	return len(db.tables)
}

// Iterator iterates keys in ascending order (memtable + SSTs merged).
type Iterator struct {
	db      *DB
	items   []iterItem
	idx     int
	release func()
}

type iterItem struct {
	key     []byte
	value   []byte
	deleted bool
	seq     uint64
}

// NewIterator returns a snapshot iterator over visible keys (tombstones hidden).
func (db *DB) NewIterator() *Iterator {
	db.mu.RLock()
	items := db.collectMerged()
	it := &Iterator{db: db, items: items, idx: -1, release: func() { db.mu.RUnlock() }}
	return it
}

func (db *DB) collectMerged() []iterItem {
	type cand struct {
		key     []byte
		value   []byte
		seq     uint64
		deleted bool
	}
	m := map[string]cand{}
	update := func(k, v []byte, seq uint64, del bool) {
		sk := string(k)
		if cur, ok := m[sk]; ok && cur.seq >= seq {
			return
		}
		m[sk] = cand{key: byteutil.Clone(k), value: byteutil.Clone(v), seq: seq, deleted: del}
	}
	for _, e := range db.mem.Snapshot() {
		update(e.Key, e.Value, e.Seq, e.Deleted)
	}
	for _, t := range db.tables {
		it := t.NewIterator()
		for it.Valid() {
			update(it.Key(), it.Value(), it.Seq(), it.Deleted())
			it.Next()
		}
	}
	keys := make([]string, 0, len(m))
	for k, c := range m {
		if c.deleted {
			continue
		}
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]iterItem, 0, len(keys))
	for _, k := range keys {
		c := m[k]
		out = append(out, iterItem{key: c.key, value: c.value, seq: c.seq, deleted: false})
	}
	return out
}

// SeekToFirst positions at the first key.
func (it *Iterator) SeekToFirst() {
	if len(it.items) == 0 {
		it.idx = -1
		return
	}
	it.idx = 0
}

// Seek positions at first key >= target.
func (it *Iterator) Seek(target []byte) {
	it.idx = sort.Search(len(it.items), func(i int) bool {
		return bytes.Compare(it.items[i].key, target) >= 0
	})
	if it.idx >= len(it.items) {
		it.idx = -1
	}
}

// Valid reports whether the iterator is positioned on a key.
func (it *Iterator) Valid() bool {
	return it.idx >= 0 && it.idx < len(it.items)
}

// Next advances.
func (it *Iterator) Next() {
	if it.idx < 0 {
		return
	}
	it.idx++
	if it.idx >= len(it.items) {
		it.idx = -1
	}
}

// Key returns the current key.
func (it *Iterator) Key() []byte {
	if !it.Valid() {
		return nil
	}
	return byteutil.Clone(it.items[it.idx].key)
}

// Value returns the current value.
func (it *Iterator) Value() []byte {
	if !it.Valid() {
		return nil
	}
	return byteutil.Clone(it.items[it.idx].value)
}

// Close releases the iterator (unlocks DB).
func (it *Iterator) Close() {
	if it.release != nil {
		it.release()
		it.release = nil
	}
}

// Has reports whether key exists (not deleted).
func (db *DB) Has(key []byte) (bool, error) {
	_, err := db.Get(key)
	if err == ErrNotFound {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return true, nil
}

// PathFor returns an absolute path under the DB directory.
func (db *DB) PathFor(name string) string {
	return filepath.Join(db.dir, name)
}

// String returns a short description.
func (db *DB) String() string {
	st := db.Stats()
	return fmt.Sprintf("lsmkv.DB{dir=%q mem=%d sst=%d wal=%d}", db.dir, st.MemtableKeys, st.SSTFiles, st.WALSize)
}
