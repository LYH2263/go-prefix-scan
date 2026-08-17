// Package compact merges SSTables while respecting sequence numbers and tombstones.
package compact

import (
	"container/heap"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/LYH2263/go-prefix-scan/internal/byteutil"
	"github.com/LYH2263/go-prefix-scan/internal/sstable"
)

var (
	ErrNoInputs = errors.New("compact: no inputs")
	ErrInvalid  = errors.New("compact: invalid argument")
)

// Input describes an SSTable to merge.
type Input struct {
	Path  string
	Level int
}

// Result describes the output of a compaction.
type Result struct {
	OutputPath      string
	Meta            *sstable.Meta
	Removed         []string
	KeptTombstones  int
	DroppedObsolete int
}

// Options configures compaction.
type Options struct {
	// DropTombstones drops tombstones from the output (dangerous unless no lower data).
	DropTombstones bool
	OutputLevel    int
	BlockSize      int
}

// MergeFiles merges the given SST files into outPath.
// For equal keys, the highest Seq wins; a winning tombstone is written unless DropTombstones.
func MergeFiles(inputs []Input, outPath string, opts *Options) (*Result, error) {
	if len(inputs) == 0 {
		return nil, ErrNoInputs
	}
	o := Options{}
	if opts != nil {
		o = *opts
	}
	readers := make([]*sstable.Reader, 0, len(inputs))
	paths := make([]string, 0, len(inputs))
	for _, in := range inputs {
		r, err := sstable.Open(in.Path)
		if err != nil {
			for _, rr := range readers {
				_ = rr.Close()
			}
			return nil, err
		}
		readers = append(readers, r)
		paths = append(paths, in.Path)
	}
	defer func() {
		for _, r := range readers {
			_ = r.Close()
		}
	}()

	h := &mergeHeap{}
	heap.Init(h)
	iters := make([]*sstable.Iterator, len(readers))
	for i, r := range readers {
		it := r.NewIterator()
		iters[i] = it
		if it.Valid() {
			heap.Push(h, &mergeItem{
				idx: i, key: it.Key(), seq: it.Seq(),
				deleted: it.Deleted(), value: it.Value(),
			})
		}
	}

	w, err := sstable.Create(outPath, &sstable.WriterOptions{BlockSize: o.BlockSize})
	if err != nil {
		return nil, err
	}

	var (
		keptTomb int
		dropped  int
		lastKey  []byte
	)

	for h.Len() > 0 {
		item := heap.Pop(h).(*mergeItem)
		// Advance source iterator first so we can push the next item.
		iters[item.idx].Next()
		if iters[item.idx].Valid() {
			nit := iters[item.idx]
			heap.Push(h, &mergeItem{
				idx: item.idx, key: nit.Key(), seq: nit.Seq(),
				deleted: nit.Deleted(), value: nit.Value(),
			})
		}

		if lastKey != nil && byteutil.Equal(item.key, lastKey) {
			// Obsolete version (heap yields higher seq first for same key).
			dropped++
			continue
		}
		lastKey = byteutil.Clone(item.key)

		if item.deleted && o.DropTombstones {
			dropped++
			continue
		}
		e := sstable.Entry{
			Key:     item.key,
			Value:   item.value,
			Seq:     item.seq,
			Deleted: item.deleted,
		}
		if e.Deleted {
			keptTomb++
		}
		if err := w.Add(e); err != nil {
			_ = w.Abort()
			return nil, err
		}
	}

	meta, err := w.Finish()
	if err != nil {
		return nil, err
	}
	return &Result{
		OutputPath:      outPath,
		Meta:            meta,
		Removed:         paths,
		KeptTombstones:  keptTomb,
		DroppedObsolete: dropped,
	}, nil
}

type mergeItem struct {
	idx     int
	key     []byte
	value   []byte
	seq     uint64
	deleted bool
}

type mergeHeap []*mergeItem

func (h mergeHeap) Len() int { return len(h) }
func (h mergeHeap) Less(i, j int) bool {
	c := byteutil.Compare(h[i].key, h[j].key)
	if c != 0 {
		return c < 0
	}
	if h[i].seq != h[j].seq {
		return h[i].seq > h[j].seq
	}
	return h[i].idx < h[j].idx
}
func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *mergeHeap) Push(x any)   { *h = append(*h, x.(*mergeItem)) }
func (h *mergeHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[:n-1]
	return item
}

// PlanLevel0 selects all level-0 files for compaction when count >= threshold.
func PlanLevel0(files []FileRef, threshold int) []Input {
	if threshold <= 0 {
		threshold = 4
	}
	var out []Input
	for _, f := range files {
		if f.Level == 0 {
			out = append(out, Input{Path: f.Path, Level: 0})
		}
	}
	if len(out) < threshold {
		return nil
	}
	return out
}

// FileRef is a lightweight file descriptor for planning.
type FileRef struct {
	Path  string
	Name  string
	Level int
	Size  int64
}

// CompactDir merges L0 (or any two) SSTs in dir into a new file named by allocName.
func CompactDir(dir string, files []FileRef, allocName func() (string, error), opts *Options) (*Result, error) {
	inputs := PlanLevel0(files, 2)
	if len(inputs) == 0 {
		if len(files) < 2 {
			return nil, ErrNoInputs
		}
		inputs = []Input{
			{Path: files[0].Path, Level: files[0].Level},
			{Path: files[1].Path, Level: files[1].Level},
		}
	}
	name, err := allocName()
	if err != nil {
		return nil, err
	}
	outPath := filepath.Join(dir, name)
	res, err := MergeFiles(inputs, outPath, opts)
	if err != nil {
		_ = os.Remove(outPath)
		return nil, err
	}
	return res, nil
}

// ValidateSorted checks that entries are ascending by key.
func ValidateSorted(entries []sstable.Entry) error {
	for i := 1; i < len(entries); i++ {
		if byteutil.Compare(entries[i-1].Key, entries[i].Key) > 0 {
			return fmt.Errorf("compact: not sorted at %d", i)
		}
	}
	return nil
}

// PickBySize returns up to n files with smallest sizes.
func PickBySize(files []FileRef, n int) []FileRef {
	if n <= 0 || len(files) == 0 {
		return nil
	}
	cp := append([]FileRef(nil), files...)
	for i := 0; i < len(cp); i++ {
		for j := i + 1; j < len(cp); j++ {
			if cp[j].Size < cp[i].Size {
				cp[i], cp[j] = cp[j], cp[i]
			}
		}
	}
	if n > len(cp) {
		n = len(cp)
	}
	return cp[:n]
}
