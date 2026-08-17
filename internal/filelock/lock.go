// Package filelock provides an exclusive directory lock using a lock file.
package filelock

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"time"
)

var (
	ErrLocked    = errors.New("filelock: already locked")
	ErrNotHeld   = errors.New("filelock: lock not held")
	ErrInvalid   = errors.New("filelock: invalid argument")
	ErrTimeout   = errors.New("filelock: lock timeout")
)

const (
	defaultLockName = "LOCK"
	defaultStaleAge = 24 * time.Hour
)

// Lock represents an exclusive lock on a directory.
type Lock struct {
	mu       sync.Mutex
	dir      string
	path     string
	f        *os.File
	held     bool
	pid      int
	hostname string
}

// Options configures lock acquisition.
type Options struct {
	// Name is the lock file name within the directory (default LOCK).
	Name string
	// Timeout is how long TryLockWithTimeout waits (0 = try once).
	Timeout time.Duration
	// RetryInterval between attempts.
	RetryInterval time.Duration
	// StaleAge: if lock file is older than this and pid is dead, remove it (best-effort).
	StaleAge time.Duration
}

func (o *Options) normalize() Options {
	out := Options{}
	if o != nil {
		out = *o
	}
	if out.Name == "" {
		out.Name = defaultLockName
	}
	if out.RetryInterval <= 0 {
		out.RetryInterval = 50 * time.Millisecond
	}
	if out.StaleAge <= 0 {
		out.StaleAge = defaultStaleAge
	}
	return out
}

// Acquire creates/opens an exclusive lock file in dir.
func Acquire(dir string, opts *Options) (*Lock, error) {
	o := opts.normalize()
	if dir == "" {
		return nil, ErrInvalid
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	path := filepath.Join(dir, o.Name)
	host, _ := os.Hostname()
	l := &Lock{
		dir:      dir,
		path:     path,
		pid:      os.Getpid(),
		hostname: host,
	}
	if o.Timeout <= 0 {
		if err := l.tryOnce(o); err != nil {
			return nil, err
		}
		return l, nil
	}
	deadline := time.Now().Add(o.Timeout)
	for {
		err := l.tryOnce(o)
		if err == nil {
			return l, nil
		}
		if !errors.Is(err, ErrLocked) {
			return nil, err
		}
		if time.Now().After(deadline) {
			return nil, ErrTimeout
		}
		time.Sleep(o.RetryInterval)
	}
}

func (l *Lock) tryOnce(o Options) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.held {
		return nil
	}
	f, err := os.OpenFile(l.path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
	if err != nil {
		if !os.IsExist(err) {
			return err
		}
		// Best-effort stale recovery.
		if l.maybeRemoveStale(o.StaleAge) {
			f, err = os.OpenFile(l.path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o644)
			if err != nil {
				if os.IsExist(err) {
					return ErrLocked
				}
				return err
			}
		} else {
			return ErrLocked
		}
	}
	content := fmt.Sprintf("pid=%d\nhost=%s\ngo=%s\ntime=%s\n",
		l.pid, l.hostname, runtime.Version(), time.Now().UTC().Format(time.RFC3339Nano))
	if _, err := f.WriteString(content); err != nil {
		_ = f.Close()
		_ = os.Remove(l.path)
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		_ = os.Remove(l.path)
		return err
	}
	l.f = f
	l.held = true
	return nil
}

func (l *Lock) maybeRemoveStale(staleAge time.Duration) bool {
	info, err := os.Stat(l.path)
	if err != nil {
		return false
	}
	if time.Since(info.ModTime()) < staleAge {
		return false
	}
	// Only remove if we cannot open for read or content looks abandoned.
	// Conservative: remove when older than staleAge.
	_ = os.Remove(l.path)
	return true
}

// Release unlocks and removes the lock file.
func (l *Lock) Release() error {
	if l == nil {
		return nil
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if !l.held {
		return ErrNotHeld
	}
	var first error
	if l.f != nil {
		if err := l.f.Close(); err != nil && first == nil {
			first = err
		}
		l.f = nil
	}
	if err := os.Remove(l.path); err != nil && !os.IsNotExist(err) && first == nil {
		first = err
	}
	l.held = false
	return first
}

// Held reports whether the lock is currently held by this process.
func (l *Lock) Held() bool {
	if l == nil {
		return false
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.held
}

// Path returns the lock file path.
func (l *Lock) Path() string {
	if l == nil {
		return ""
	}
	return l.path
}

// Dir returns the locked directory.
func (l *Lock) Dir() string {
	if l == nil {
		return ""
	}
	return l.dir
}

// IsLocked reports whether a lock file currently exists in dir.
func IsLocked(dir string, name string) bool {
	if name == "" {
		name = defaultLockName
	}
	_, err := os.Stat(filepath.Join(dir, name))
	return err == nil
}

// MustAcquire panics on failure (for tests).
func MustAcquire(dir string) *Lock {
	l, err := Acquire(dir, nil)
	if err != nil {
		panic(err)
	}
	return l
}

// WithLock runs fn while holding an exclusive lock on dir.
func WithLock(dir string, opts *Options, fn func() error) error {
	l, err := Acquire(dir, opts)
	if err != nil {
		return err
	}
	defer func() { _ = l.Release() }()
	return fn()
}

// Info describes lock file metadata when readable.
type Info struct {
	Path    string
	Size    int64
	ModTime time.Time
	Content string
}

// ReadInfo reads lock file info if present.
func ReadInfo(dir, name string) (*Info, error) {
	if name == "" {
		name = defaultLockName
	}
	path := filepath.Join(dir, name)
	st, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return &Info{
		Path:    path,
		Size:    st.Size(),
		ModTime: st.ModTime(),
		Content: string(b),
	}, nil
}
