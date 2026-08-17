// Package manifest tracks the set of live SSTable files for an LSM database.
package manifest

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/LYH2263/go-prefix-scan/internal/byteutil"
	"github.com/LYH2263/go-prefix-scan/internal/crc32x"
)

var (
	ErrCorrupt = errors.New("manifest: corrupt")
	ErrClosed  = errors.New("manifest: closed")
)

const (
	fileName     = "MANIFEST"
	tmpSuffix    = ".tmp"
	magicBinary  = uint32(0x4D4E4653) // MNFS
	versionJSON  = 1
)

// FileMeta describes one SSTable in the manifest.
type FileMeta struct {
	Name     string `json:"name"`
	Level    int    `json:"level"`
	MinKey   []byte `json:"min_key"`
	MaxKey   []byte `json:"max_key"`
	MaxSeq   uint64 `json:"max_seq"`
	Entries  uint64 `json:"entries"`
	FileSize int64  `json:"file_size"`
}

// State is the durable manifest content.
type State struct {
	Version   int        `json:"version"`
	NextFile  uint64     `json:"next_file"`
	LogSeq    uint64     `json:"log_seq"`
	Files     []FileMeta `json:"files"`
	UpdatedAt string     `json:"updated_at"`
}

// Manifest manages atomic updates to the MANIFEST file.
type Manifest struct {
	mu    sync.Mutex
	dir   string
	path  string
	state State
}

// Open loads or creates a manifest in dir.
func Open(dir string) (*Manifest, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, fileName)
	m := &Manifest{dir: dir, path: path}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			m.state = State{Version: versionJSON, NextFile: 1, Files: nil, UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano)}
			if err := m.persistLocked(); err != nil {
				return nil, err
			}
			return m, nil
		}
		return nil, err
	}
	st, err := decode(data)
	if err != nil {
		return nil, err
	}
	m.state = st
	return m, nil
}

func decode(data []byte) (State, error) {
	if len(data) >= 4 && binary.LittleEndian.Uint32(data[:4]) == magicBinary {
		return decodeBinary(data)
	}
	var st State
	if err := json.Unmarshal(data, &st); err != nil {
		return State{}, ErrCorrupt
	}
	if st.Version == 0 {
		st.Version = versionJSON
	}
	return st, nil
}

func decodeBinary(data []byte) (State, error) {
	if len(data) < 4+4+8+8+4 {
		return State{}, ErrCorrupt
	}
	if binary.LittleEndian.Uint32(data[0:4]) != magicBinary {
		return State{}, ErrCorrupt
	}
	ver := binary.LittleEndian.Uint32(data[4:8])
	next := binary.LittleEndian.Uint64(data[8:16])
	logSeq := binary.LittleEndian.Uint64(data[16:24])
	n := binary.LittleEndian.Uint32(data[24:28])
	off := 28
	files := make([]FileMeta, 0, n)
	for i := uint32(0); i < n; i++ {
		if off+2 > len(data) {
			return State{}, ErrCorrupt
		}
		nameLen := int(binary.LittleEndian.Uint16(data[off : off+2]))
		off += 2
		if off+nameLen > len(data) {
			return State{}, ErrCorrupt
		}
		name := string(data[off : off+nameLen])
		off += nameLen
		if off+4+8+8+8 > len(data) {
			return State{}, ErrCorrupt
		}
		level := int(binary.LittleEndian.Uint32(data[off : off+4]))
		off += 4
		maxSeq := binary.LittleEndian.Uint64(data[off : off+8])
		off += 8
		entries := binary.LittleEndian.Uint64(data[off : off+8])
		off += 8
		fileSize := int64(binary.LittleEndian.Uint64(data[off : off+8]))
		off += 8
		if off+2 > len(data) {
			return State{}, ErrCorrupt
		}
		mkLen := int(binary.LittleEndian.Uint16(data[off : off+2]))
		off += 2
		if off+mkLen > len(data) {
			return State{}, ErrCorrupt
		}
		minKey := byteutil.Clone(data[off : off+mkLen])
		off += mkLen
		if off+2 > len(data) {
			return State{}, ErrCorrupt
		}
		xkLen := int(binary.LittleEndian.Uint16(data[off : off+2]))
		off += 2
		if off+xkLen > len(data) {
			return State{}, ErrCorrupt
		}
		maxKey := byteutil.Clone(data[off : off+xkLen])
		off += xkLen
		files = append(files, FileMeta{
			Name: name, Level: level, MinKey: minKey, MaxKey: maxKey,
			MaxSeq: maxSeq, Entries: entries, FileSize: fileSize,
		})
	}
	if off+4 > len(data) {
		return State{}, ErrCorrupt
	}
	want := binary.LittleEndian.Uint32(data[off : off+4])
	if crc32x.ChecksumCastagnoli(data[:off]) != want {
		return State{}, ErrCorrupt
	}
	return State{
		Version:   int(ver),
		NextFile:  next,
		LogSeq:    logSeq,
		Files:     files,
		UpdatedAt: time.Now().UTC().Format(time.RFC3339Nano),
	}, nil
}

func (m *Manifest) persistLocked() error {
	m.state.UpdatedAt = time.Now().UTC().Format(time.RFC3339Nano)
	m.state.Version = versionJSON
	data, err := json.MarshalIndent(m.state, "", "  ")
	if err != nil {
		return err
	}
	tmp := m.path + tmpSuffix
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	f, err := os.OpenFile(tmp, os.O_RDWR, 0)
	if err != nil {
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	_ = f.Close()
	if err := os.Rename(tmp, m.path); err != nil {
		return err
	}
	// best-effort sync directory
	if d, err := os.Open(m.dir); err == nil {
		_ = d.Sync()
		_ = d.Close()
	}
	return nil
}

// Current returns a copy of the state.
func (m *Manifest) Current() State {
	m.mu.Lock()
	defer m.mu.Unlock()
	return cloneState(m.state)
}

func cloneState(st State) State {
	out := st
	out.Files = make([]FileMeta, len(st.Files))
	for i, f := range st.Files {
		out.Files[i] = FileMeta{
			Name:     f.Name,
			Level:    f.Level,
			MinKey:   byteutil.Clone(f.MinKey),
			MaxKey:   byteutil.Clone(f.MaxKey),
			MaxSeq:   f.MaxSeq,
			Entries:  f.Entries,
			FileSize: f.FileSize,
		}
	}
	return out
}

// Files returns live file metas.
func (m *Manifest) Files() []FileMeta {
	return m.Current().Files
}

// LogSeq returns the durable log sequence watermark.
func (m *Manifest) LogSeq() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.state.LogSeq
}

// AllocFileName allocates the next SST file name like 000001.sst.
func (m *Manifest) AllocFileName() (string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	name := fmt.Sprintf("%06d.sst", m.state.NextFile)
	m.state.NextFile++
	if err := m.persistLocked(); err != nil {
		return "", err
	}
	return name, nil
}

// AddFile adds a file and optionally bumps LogSeq.
func (m *Manifest) AddFile(meta FileMeta, logSeq uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.state.Files = append(m.state.Files, meta)
	if logSeq > m.state.LogSeq {
		m.state.LogSeq = logSeq
	}
	return m.persistLocked()
}

// Replace atomically removes oldNames and adds newFiles; updates logSeq if higher.
func (m *Manifest) Replace(remove []string, add []FileMeta, logSeq uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	rm := map[string]struct{}{}
	for _, n := range remove {
		rm[n] = struct{}{}
	}
	kept := make([]FileMeta, 0, len(m.state.Files))
	for _, f := range m.state.Files {
		if _, ok := rm[f.Name]; !ok {
			kept = append(kept, f)
		}
	}
	kept = append(kept, add...)
	m.state.Files = kept
	if logSeq > m.state.LogSeq {
		m.state.LogSeq = logSeq
	}
	return m.persistLocked()
}

// SetLogSeq updates the log sequence watermark.
func (m *Manifest) SetLogSeq(seq uint64) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if seq > m.state.LogSeq {
		m.state.LogSeq = seq
	}
	return m.persistLocked()
}

// Count returns number of SST files.
func (m *Manifest) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.state.Files)
}

// MaxSeqAmongFiles returns the max MaxSeq across files.
func (m *Manifest) MaxSeqAmongFiles() uint64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	var max uint64
	for _, f := range m.state.Files {
		if f.MaxSeq > max {
			max = f.MaxSeq
		}
	}
	return max
}

// Path returns the manifest path.
func (m *Manifest) Path() string { return m.path }

// Dir returns the directory.
func (m *Manifest) Dir() string { return m.dir }

// EncodeBinary encodes state to the binary format (for tests / alternate persistence).
func EncodeBinary(st State) []byte {
	var b []byte
	hdr := make([]byte, 28)
	binary.LittleEndian.PutUint32(hdr[0:4], magicBinary)
	binary.LittleEndian.PutUint32(hdr[4:8], uint32(st.Version))
	binary.LittleEndian.PutUint64(hdr[8:16], st.NextFile)
	binary.LittleEndian.PutUint64(hdr[16:24], st.LogSeq)
	binary.LittleEndian.PutUint32(hdr[24:28], uint32(len(st.Files)))
	b = append(b, hdr...)
	for _, f := range st.Files {
		name := []byte(f.Name)
		var tmp [2]byte
		binary.LittleEndian.PutUint16(tmp[:], uint16(len(name)))
		b = append(b, tmp[:]...)
		b = append(b, name...)
		var num [4 + 8 + 8 + 8]byte
		binary.LittleEndian.PutUint32(num[0:4], uint32(f.Level))
		binary.LittleEndian.PutUint64(num[4:12], f.MaxSeq)
		binary.LittleEndian.PutUint64(num[12:20], f.Entries)
		binary.LittleEndian.PutUint64(num[20:28], uint64(f.FileSize))
		b = append(b, num[:]...)
		binary.LittleEndian.PutUint16(tmp[:], uint16(len(f.MinKey)))
		b = append(b, tmp[:]...)
		b = append(b, f.MinKey...)
		binary.LittleEndian.PutUint16(tmp[:], uint16(len(f.MaxKey)))
		b = append(b, tmp[:]...)
		b = append(b, f.MaxKey...)
	}
	sum := crc32x.ChecksumCastagnoli(b)
	var crc [4]byte
	binary.LittleEndian.PutUint32(crc[:], sum)
	b = append(b, crc[:]...)
	return b
}
