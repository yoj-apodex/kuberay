// Package driverarchive uploads append-only driver logs while their Pod is alive.
// A producer marker and an inherited flock distinguish a complete file from a
// quiet snapshot. The remote index is a CAS-updated commit record; all referenced
// checkpoints and data blocks are immutable and content addressed.
package driverarchive

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"regexp"
	"syscall"
	"time"
)

const BlockSize = 1024 * 1024
const MetadataLimit = 16 * 1024

var ErrNotFound = errors.New("archive object not found")
var ErrConflict = errors.New("archive index conflict")
var component = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_.-]{0,127}$`)
var nodePattern = regexp.MustCompile(`^[a-f0-9]{56}$`)
var generationPattern = regexp.MustCompile(`^[a-f0-9]{32}$`)
var digestPattern = regexp.MustCompile(`^[a-f0-9]{64}$`)

// Store must enforce conditional updates remotely, including across restarts.
// expected="" creates an object only if absent; otherwise it is an opaque ETag.
type Store interface {
	Get(context.Context, string, int64) ([]byte, string, error)
	Put(context.Context, string, []byte, string) (string, error)
}

type Marker struct {
	Version    int    `json:"version"`
	Sealed     bool   `json:"sealed"`
	Invalid    bool   `json:"invalid,omitempty"`
	Submission string `json:"submission"`
	Session    string `json:"session"`
	Node       string `json:"node"`
	PodUID     string `json:"pod_uid"`
	Generation string `json:"generation"`
	Device     uint64 `json:"device"`
	Inode      uint64 `json:"inode"`
}

func (m Marker) Validate() error {
	if m.Version != 2 || !component.MatchString(m.Submission) || !component.MatchString(m.Session) || !nodePattern.MatchString(m.Node) || !component.MatchString(m.PodUID) || !generationPattern.MatchString(m.Generation) {
		return errors.New("invalid driver archive marker")
	}
	return nil
}

type Index struct {
	ModifiedNS        int64  `json:"modified_ns"`
	Parts             int64  `json:"parts"`
	CompactBytes      int64  `json:"compact_bytes"`
	CompactCheckpoint string `json:"compact_checkpoint"`
	Finalizing        bool   `json:"finalizing"`
	Marker
	Bytes      int64  `json:"bytes"`
	Observed   int64  `json:"observed_bytes"`
	Checkpoint string `json:"checkpoint"`
	Complete   bool   `json:"complete"`
	Error      string `json:"error,omitempty"`
	// Earlier than the bucket's 365-day per-object expiry. Never extended on append.
	ReadableUntil int64 `json:"readable_until"`
}
type Checkpoint struct {
	Version  int    `json:"version"`
	Start    int64  `json:"start"`
	End      int64  `json:"end"`
	Data     string `json:"data"`
	Previous string `json:"previous"`
}

type Engine struct {
	Store  Store
	Prefix string
	Now    func() time.Time
}

func Digest(b []byte) string { s := sha256.Sum256(b); return hex.EncodeToString(s[:]) }
func (e *Engine) Base(m Marker) string {
	return path.Join(e.Prefix, "archive-v2", m.Node, m.Submission)
}
func (e *Engine) immutable(ctx context.Context, key string, data []byte) error {
	_, err := e.Store.Put(ctx, key, data, "")
	if errors.Is(err, ErrConflict) {
		prior, _, readErr := e.Store.Get(ctx, key, int64(len(data)))
		if readErr != nil {
			return readErr
		}
		if Digest(prior) != Digest(data) {
			return errors.New("immutable archive object mismatch")
		}
		return nil
	}
	return err
}
func encode(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return b
}

// Step uploads at most one bounded block. Calling it again resumes from the
// remote commit, not a local cursor. The caller serializes steps and rate limits
// scheduling. A flock is held only for fstat, never for network IO.
func (e *Engine) Step(ctx context.Context, filename string, m Marker) (Index, error) {
	var index Index
	if err := m.Validate(); err != nil {
		return index, err
	}
	fd, err := syscall.Open(filename, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return index, err
	}
	f := os.NewFile(uintptr(fd), filename)
	defer f.Close()
	stat, err := f.Stat()
	if err != nil {
		return index, err
	}
	sys, ok := stat.Sys().(*syscall.Stat_t)
	identityChanged := !ok || !stat.Mode().IsRegular() || uint64(sys.Dev) != m.Device || sys.Ino != m.Inode
	closed := false
	if err := syscall.Flock(fd, syscall.LOCK_EX|syscall.LOCK_NB); err == nil {
		closed = true
		stat, err = f.Stat()
		_ = syscall.Flock(fd, syscall.LOCK_UN)
		if err != nil {
			return index, err
		}
	} else if !errors.Is(err, syscall.EWOULDBLOCK) && !errors.Is(err, syscall.EAGAIN) {
		return index, err
	}
	size := stat.Size()
	base := e.Base(m)
	key := path.Join(base, "index.json")
	body, etag, err := e.Store.Get(ctx, key, MetadataLimit)
	if errors.Is(err, ErrNotFound) {
		now := time.Now()
		if e.Now != nil {
			now = e.Now()
		}
		index = Index{Marker: m, Observed: size, ReadableUntil: now.Add(364 * 24 * time.Hour).Unix()}
		etag, err = e.Store.Put(ctx, key, encode(index), "")
		if err != nil {
			return index, err
		}
	} else if err != nil {
		return index, err
	} else if err = json.Unmarshal(body, &index); err != nil {
		return index, err
	}
	identityBefore, identityNow := index.Marker, m
	identityBefore.Sealed = false
	identityNow.Sealed = false
	identityBefore.Invalid = false
	identityNow.Invalid = false
	if identityBefore != identityNow {
		return index, errors.New("archive identity collision")
	}
	if index.Bytes < 0 || index.Bytes > index.Observed || (index.Bytes == 0) != (index.Checkpoint == "") || (index.Checkpoint != "" && !digestPattern.MatchString(index.Checkpoint)) {
		return index, errors.New("invalid remote archive index")
	}
	if index.Error != "" {
		return index, fmt.Errorf("archive blocked: %s", index.Error)
	}
	if identityChanged || m.Invalid || size < index.Bytes || (index.Complete && (size != index.Bytes || !m.Sealed || stat.ModTime().UnixNano() != index.ModifiedNS)) || (index.Finalizing && (size != index.Bytes || !closed || !m.Sealed || stat.ModTime().UnixNano() != index.ModifiedNS)) {
		index.Error = "source_changed"
		_, err = e.Store.Put(ctx, key, encode(index), etag)
		if err != nil {
			return index, err
		}
		return index, errors.New("driver log changed after commit")
	}
	if index.Complete {
		return index, nil
	}
	added := size > index.Bytes
	if added {
		length := min(int64(BlockSize), size-index.Bytes)
		data := make([]byte, length)
		if _, err = io.ReadFull(io.NewSectionReader(f, index.Bytes, length), data); err != nil {
			return index, err
		}
		digest := Digest(data)
		if err = e.immutable(ctx, path.Join(base, m.Generation, "chunks", digest), data); err != nil {
			return index, err
		}
		checkpoint := Checkpoint{Version: 2, Start: index.Bytes, End: index.Bytes + length, Data: digest, Previous: index.Checkpoint}
		encoded := encode(checkpoint)
		hash := Digest(encoded)
		if err = e.immutable(ctx, path.Join(base, m.Generation, "checkpoints", hash), encoded); err != nil {
			return index, err
		}
		index.Bytes += length
		index.Checkpoint = hash
		index.Parts++
	}
	// Detect a truncate or replacement observed during the read. Arbitrary writes
	// bypassing the producer flock protocol are not supported append-only logs.
	after, err := f.Stat()
	if err != nil {
		return index, err
	}
	if after.Size() < size {
		return index, errors.New("driver file truncated during upload")
	}
	index.Observed = after.Size()
	index.ModifiedNS = after.ModTime().UnixNano()
	index.Sealed = m.Sealed
	index.Complete = closed && m.Sealed && index.Bytes == size && after.Size() == size && after.ModTime() == stat.ModTime()

	// Frequent tiny uploads would otherwise require thousands of GETs to read a
	// small completed log. Once the writer closes, compact once into <=1MiB blocks.
	// Raw confirmed progress remains intact until the final chain is committed.
	if index.Complete && (index.Parts > (size+BlockSize-1)/BlockSize+8 || index.Finalizing) {
		index.Complete = false
		index.Finalizing = true
		if !added {
			if index.CompactBytes < 0 || index.CompactBytes > size || (index.CompactBytes == 0) != (index.CompactCheckpoint == "") || (index.CompactCheckpoint != "" && !digestPattern.MatchString(index.CompactCheckpoint)) {
				return index, errors.New("invalid compaction offset")
			}
			length := min(int64(BlockSize), size-index.CompactBytes)
			if length > 0 {
				data := make([]byte, length)
				if _, err = io.ReadFull(io.NewSectionReader(f, index.CompactBytes, length), data); err != nil {
					return index, err
				}
				digest := Digest(data)
				if err = e.immutable(ctx, path.Join(base, m.Generation, "chunks", digest), data); err != nil {
					return index, err
				}
				cp := Checkpoint{Version: 2, Start: index.CompactBytes, End: index.CompactBytes + length, Data: digest, Previous: index.CompactCheckpoint}
				encoded := encode(cp)
				hash := Digest(encoded)
				if err = e.immutable(ctx, path.Join(base, m.Generation, "checkpoints", hash), encoded); err != nil {
					return index, err
				}
				index.CompactBytes += length
				index.CompactCheckpoint = hash
			}
			if index.CompactBytes == size {
				index.Checkpoint = index.CompactCheckpoint
				index.Complete = true
				index.Finalizing = false
				index.Parts = (size + BlockSize - 1) / BlockSize
			}
		}
	}
	if index.Complete || index.Finalizing {
		latest, statErr := f.Stat()
		if statErr != nil {
			return index, statErr
		}
		if latest.Size() != size || latest.ModTime() != stat.ModTime() {
			index.Complete = false
			index.Error = "source_changed_during_finalization"
		}
	}
	if string(encode(index)) == string(body) {
		return index, nil
	}

	_, err = e.Store.Put(ctx, key, encode(index), etag)
	return index, err
}
