// Package memtable implements an in-memory skiplist memtable for LSM writes.
package memtable

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"github.com/LYH2263/go-prefix-scan/internal/byteutil"
)

const (
	maxLevel    = 16
	pNumerator  = 1
	pDenominator = 4
)

// Entry is a key/value with sequence and tombstone flag.
type Entry struct {
	Key     []byte
	Value   []byte
	Seq     uint64
	Deleted bool
}

type node struct {
	entry Entry
	next  []*node
}

// SkipList is a concurrent-read / single-writer-friendly skiplist (mutex protected).
type SkipList struct {
	mu       sync.RWMutex
	head     *node
	level    int
	size     int
	bytes    int64
	rnd      *rand.Rand
	rndMu    sync.Mutex
}

// New creates an empty skiplist memtable.
func New() *SkipList {
	h := &node{next: make([]*node, maxLevel)}
	return &SkipList{
		head:  h,
		level: 1,
		rnd:   rand.New(rand.NewSource(time.Now().UnixNano())),
	}
}

func (s *SkipList) randomLevel() int {
	s.rndMu.Lock()
	defer s.rndMu.Unlock()
	lvl := 1
	for lvl < maxLevel && s.rnd.Intn(pDenominator) < pNumerator {
		lvl++
	}
	return lvl
}

// compare keys; higher seq wins when keys equal (for internal ordering we keep one per key).
func compareKey(a, b []byte) int {
	return bytes.Compare(a, b)
}

// Put inserts or replaces a value for key.
func (s *SkipList) Put(key, value []byte, seq uint64) {
	s.set(key, value, seq, false)
}

// Delete inserts a tombstone for key.
func (s *SkipList) Delete(key []byte, seq uint64) {
	s.set(key, nil, seq, true)
}

func (s *SkipList) set(key, value []byte, seq uint64, deleted bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	update := make([]*node, maxLevel)
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && compareKey(x.next[i].entry.Key, key) < 0 {
			x = x.next[i]
		}
		update[i] = x
	}
	x = x.next[0]
	if x != nil && compareKey(x.entry.Key, key) == 0 {
		// replace if newer seq
		if seq >= x.entry.Seq {
			oldBytes := estimateBytes(x.entry)
			x.entry.Value = byteutil.Clone(value)
			x.entry.Seq = seq
			x.entry.Deleted = deleted
			s.bytes += estimateBytes(x.entry) - oldBytes
		}
		return
	}
	lvl := s.randomLevel()
	if lvl > s.level {
		for i := s.level; i < lvl; i++ {
			update[i] = s.head
		}
		s.level = lvl
	}
	n := &node{
		entry: Entry{
			Key:     byteutil.Clone(key),
			Value:   byteutil.Clone(value),
			Seq:     seq,
			Deleted: deleted,
		},
		next: make([]*node, lvl),
	}
	for i := 0; i < lvl; i++ {
		n.next[i] = update[i].next[i]
		update[i].next[i] = n
	}
	s.size++
	s.bytes += estimateBytes(n.entry)
}

func estimateBytes(e Entry) int64 {
	return int64(len(e.Key) + len(e.Value) + 16)
}

// Get returns the entry for key if present.
func (s *SkipList) Get(key []byte) (Entry, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	x := s.head
	for i := s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && compareKey(x.next[i].entry.Key, key) < 0 {
			x = x.next[i]
		}
	}
	x = x.next[0]
	if x != nil && compareKey(x.entry.Key, key) == 0 {
		e := x.entry
		e.Key = byteutil.Clone(e.Key)
		e.Value = byteutil.Clone(e.Value)
		return e, true
	}
	return Entry{}, false
}

// Len returns the number of keys.
func (s *SkipList) Len() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.size
}

// ApproxBytes returns approximate memory usage.
func (s *SkipList) ApproxBytes() int64 {
	return atomic.LoadInt64(&s.bytes)
}

// Bytes returns approximate memory usage (locked).
func (s *SkipList) Bytes() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.bytes
}

// Clear removes all entries.
func (s *SkipList) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.head = &node{next: make([]*node, maxLevel)}
	s.level = 1
	s.size = 0
	s.bytes = 0
}

// Iterator walks keys in ascending order.
type Iterator struct {
	s   *SkipList
	cur *node
}

// NewIterator returns an iterator positioned before the first element.
func (s *SkipList) NewIterator() *Iterator {
	s.mu.RLock()
	// hold RLock for the lifetime of iteration via Snapshot approach:
	// copy entries for safety instead of holding lock.
	s.mu.RUnlock()
	return &Iterator{s: s, cur: s.head}
}

// SeekToFirst positions at the smallest key.
func (it *Iterator) SeekToFirst() {
	it.s.mu.RLock()
	defer it.s.mu.RUnlock()
	it.cur = it.s.head.next[0]
}

// Seek positions at the first key >= target.
func (it *Iterator) Seek(target []byte) {
	it.s.mu.RLock()
	defer it.s.mu.RUnlock()
	x := it.s.head
	for i := it.s.level - 1; i >= 0; i-- {
		for x.next[i] != nil && compareKey(x.next[i].entry.Key, target) < 0 {
			x = x.next[i]
		}
	}
	it.cur = x.next[0]
}

// Valid reports whether the iterator is on an entry.
func (it *Iterator) Valid() bool {
	return it.cur != nil
}

// Next advances to the next entry.
func (it *Iterator) Next() {
	if it.cur == nil {
		return
	}
	it.s.mu.RLock()
	defer it.s.mu.RUnlock()
	it.cur = it.cur.next[0]
}

// Key returns the current key (cloned).
func (it *Iterator) Key() []byte {
	if it.cur == nil {
		return nil
	}
	return byteutil.Clone(it.cur.entry.Key)
}

// Value returns the current value (cloned).
func (it *Iterator) Value() []byte {
	if it.cur == nil {
		return nil
	}
	return byteutil.Clone(it.cur.entry.Value)
}

// Seq returns the current sequence.
func (it *Iterator) Seq() uint64 {
	if it.cur == nil {
		return 0
	}
	return it.cur.entry.Seq
}

// Deleted reports tombstone.
func (it *Iterator) Deleted() bool {
	if it.cur == nil {
		return false
	}
	return it.cur.entry.Deleted
}

// Entry returns a copy of the current entry.
func (it *Iterator) Entry() Entry {
	if it.cur == nil {
		return Entry{}
	}
	e := it.cur.entry
	e.Key = byteutil.Clone(e.Key)
	e.Value = byteutil.Clone(e.Value)
	return e
}

// Snapshot returns all entries in order (owned copies).
func (s *SkipList) Snapshot() []Entry {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]Entry, 0, s.size)
	for n := s.head.next[0]; n != nil; n = n.next[0] {
		e := n.entry
		e.Key = byteutil.Clone(e.Key)
		e.Value = byteutil.Clone(e.Value)
		out = append(out, e)
	}
	return out
}

// Ascend calls fn for each entry in order; stop if fn returns false.
func (s *SkipList) Ascend(fn func(Entry) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	for n := s.head.next[0]; n != nil; n = n.next[0] {
		e := n.entry
		e.Key = byteutil.Clone(e.Key)
		e.Value = byteutil.Clone(e.Value)
		if !fn(e) {
			return
		}
	}
}

// AscendRange iterates keys in [lo, hi).
func (s *SkipList) AscendRange(lo, hi []byte, fn func(Entry) bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	x := s.head
	if lo != nil {
		for i := s.level - 1; i >= 0; i-- {
			for x.next[i] != nil && compareKey(x.next[i].entry.Key, lo) < 0 {
				x = x.next[i]
			}
		}
	}
	for n := x.next[0]; n != nil; n = n.next[0] {
		if hi != nil && compareKey(n.entry.Key, hi) >= 0 {
			return
		}
		e := n.entry
		e.Key = byteutil.Clone(e.Key)
		e.Value = byteutil.Clone(e.Value)
		if !fn(e) {
			return
		}
	}
}

// MaxSeq returns the maximum sequence number in the table.
func (s *SkipList) MaxSeq() uint64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var max uint64
	for n := s.head.next[0]; n != nil; n = n.next[0] {
		if n.entry.Seq > max {
			max = n.entry.Seq
		}
	}
	return max
}

// MemTable wraps SkipList with a flush-size threshold helper.
type MemTable struct {
	sl       *SkipList
	maxBytes int64
}

// NewMemTable creates a memtable with a soft size limit.
func NewMemTable(maxBytes int64) *MemTable {
	if maxBytes <= 0 {
		maxBytes = 4 << 20
	}
	return &MemTable{sl: New(), maxBytes: maxBytes}
}

// Underlying returns the skiplist.
func (m *MemTable) Underlying() *SkipList { return m.sl }

// Put delegates to skiplist.
func (m *MemTable) Put(key, value []byte, seq uint64) { m.sl.Put(key, value, seq) }

// Delete delegates to skiplist.
func (m *MemTable) Delete(key []byte, seq uint64) { m.sl.Delete(key, seq) }

// Get delegates to skiplist.
func (m *MemTable) Get(key []byte) (Entry, bool) { return m.sl.Get(key) }

// ShouldFlush reports whether ApproxBytes exceeds the limit.
func (m *MemTable) ShouldFlush() bool {
	return m.sl.Bytes() >= m.maxBytes
}

// Bytes returns approximate size.
func (m *MemTable) Bytes() int64 { return m.sl.Bytes() }

// Len returns entry count.
func (m *MemTable) Len() int { return m.sl.Len() }

// Snapshot returns all entries.
func (m *MemTable) Snapshot() []Entry { return m.sl.Snapshot() }

// Clear empties the table.
func (m *MemTable) Clear() { m.sl.Clear() }

// MaxSeq returns max sequence.
func (m *MemTable) MaxSeq() uint64 { return m.sl.MaxSeq() }

// NewIterator returns an iterator.
func (m *MemTable) NewIterator() *Iterator { return m.sl.NewIterator() }
