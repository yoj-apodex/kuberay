package driverarchive

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

type memoryStore struct {
	objects      map[string][]byte
	versions     map[string]int
	fail         string
	lostResponse bool
}

func newStore() *memoryStore {
	return &memoryStore{objects: map[string][]byte{}, versions: map[string]int{}}
}
func (s *memoryStore) Get(ctx context.Context, k string, n int64) ([]byte, string, error) {
	if err := ctx.Err(); err != nil {
		return nil, "", err
	}
	v, ok := s.objects[k]
	if !ok {
		return nil, "", ErrNotFound
	}
	if int64(len(v)) > n {
		return nil, "", errors.New("too large")
	}
	return append([]byte(nil), v...), fmt.Sprint(s.versions[k]), nil
}
func (s *memoryStore) Put(ctx context.Context, k string, b []byte, expected string) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}
	if s.fail != "" && strings.Contains(k, s.fail) {
		return "", errors.New("injected upload failure")
	}
	version := s.versions[k]
	if (expected == "" && version != 0) || (expected != "" && expected != fmt.Sprint(version)) {
		return "", ErrConflict
	}
	s.objects[k] = append([]byte(nil), b...)
	s.versions[k]++
	if s.lostResponse && strings.HasSuffix(k, "index.json") {
		s.lostResponse = false
		return "", errors.New("response lost")
	}
	return fmt.Sprint(s.versions[k]), nil
}
func fixture(t *testing.T) (*Engine, *memoryStore, string, Marker) {
	t.Helper()
	name := filepath.Join(t.TempDir(), "job-driver-test.log")
	if err := os.WriteFile(name, []byte("first\n"), 0600); err != nil {
		t.Fatal(err)
	}
	st, _ := os.Stat(name)
	sys := st.Sys().(*syscall.Stat_t)
	m := Marker{Version: 2, Submission: "test", Session: "session_test", Node: strings.Repeat("a", 56), PodUID: "pod-test", Generation: strings.Repeat("b", 32), Device: uint64(sys.Dev), Inode: sys.Ino}
	s := newStore()
	return &Engine{Store: s, Prefix: "ray-history/cluster-history/raycluster/ray-workloads/ray-test"}, s, name, m
}
func decodeAll(t *testing.T, e *Engine, s *memoryStore, m Marker, index Index) []byte {
	t.Helper()
	next := index.Checkpoint
	end := index.Bytes
	var parts [][]byte
	for next != "" {
		raw := s.objects[e.Base(m)+"/"+m.Generation+"/checkpoints/"+next]
		if Digest(raw) != next {
			t.Fatal("bad checkpoint")
		}
		var cp Checkpoint
		if err := json.Unmarshal(raw, &cp); err != nil {
			t.Fatal(err)
		}
		data := s.objects[e.Base(m)+"/"+m.Generation+"/chunks/"+cp.Data]
		if Digest(data) != cp.Data || cp.End != end || int64(len(data)) != cp.End-cp.Start {
			t.Fatal("bad chain")
		}
		parts = append(parts, data)
		end = cp.Start
		next = cp.Previous
	}
	if end != 0 {
		t.Fatal("gap")
	}
	var out []byte
	for i := len(parts) - 1; i >= 0; i-- {
		out = append(out, parts[i]...)
	}
	return out
}
func TestActiveUploadAndLastWriterClosure(t *testing.T) {
	e, s, name, m := fixture(t)
	writer, _ := os.OpenFile(name, os.O_APPEND|os.O_WRONLY, 0600)
	defer writer.Close()
	if err := syscall.Flock(int(writer.Fd()), syscall.LOCK_SH|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	m.Sealed = true // A terminal job may still have a child holding stdout open.
	index, err := e.Step(context.Background(), name, m)
	if err != nil || index.Complete || index.Bytes != 6 {
		t.Fatalf("%+v %v", index, err)
	}
	_, _ = writer.WriteString("last\n")
	_ = writer.Close()
	index, err = e.Step(context.Background(), name, m)
	if err != nil || !index.Complete {
		t.Fatalf("%+v %v", index, err)
	}
	if got := string(decodeAll(t, e, s, m, index)); got != "first\nlast\n" {
		t.Fatal(got)
	}
}
func TestUnsealedIsNeverComplete(t *testing.T) {
	e, _, name, m := fixture(t)
	index, err := e.Step(context.Background(), name, m)
	if err != nil || index.Complete {
		t.Fatalf("%+v %v", index, err)
	}
}
func TestFailureAndLostResponseResumeFromRemote(t *testing.T) {
	for _, fail := range []string{"/chunks/", "/checkpoints/"} {
		t.Run(fail, func(t *testing.T) {
			e, s, name, m := fixture(t)
			s.fail = fail
			if _, err := e.Step(context.Background(), name, m); err == nil {
				t.Fatal("accepted failure")
			}
			var remote Index
			_ = json.Unmarshal(s.objects[e.Base(m)+"/index.json"], &remote)
			if remote.Bytes != 0 {
				t.Fatal("advanced before commit")
			}
			s.fail = ""
			m.Sealed = true
			index, err := e.Step(context.Background(), name, m)
			if err != nil || !index.Complete {
				t.Fatal(err)
			}
			if string(decodeAll(t, e, s, m, index)) != "first\n" {
				t.Fatal("wrong data")
			}
		})
	}
	e, s, name, m := fixture(t)
	s.lostResponse = true
	if _, err := e.Step(context.Background(), name, m); err == nil {
		t.Fatal("missing lost response")
	}
	m.Sealed = true
	index, err := e.Step(context.Background(), name, m)
	if err != nil || !index.Complete {
		t.Fatal(err)
	}
}
func TestBoundedLargeFileAndTruncation(t *testing.T) {
	e, s, name, m := fixture(t)
	data := strings.Repeat("a", BlockSize+19)
	_ = os.WriteFile(name, []byte(data), 0600)
	m.Sealed = true
	first, err := e.Step(context.Background(), name, m)
	if err != nil || first.Bytes != BlockSize || first.Complete {
		t.Fatalf("%+v %v", first, err)
	}
	second, err := e.Step(context.Background(), name, m)
	if err != nil || !second.Complete || string(decodeAll(t, e, s, m, second)) != data {
		t.Fatal(err)
	}
	_ = os.Truncate(name, 2)
	if _, err = e.Step(context.Background(), name, m); err == nil {
		t.Fatal("accepted truncation")
	}
	var remote Index
	_ = json.Unmarshal(s.objects[e.Base(m)+"/index.json"], &remote)
	if remote.Error != "source_changed" {
		t.Fatal(remote)
	}
}
func TestIdentityConflictDeadlineAndImmutableCollision(t *testing.T) {
	e, s, name, m := fixture(t)
	_, _ = e.Step(context.Background(), name, m)
	other := m
	other.Generation = strings.Repeat("c", 32)
	if _, err := e.Step(context.Background(), name, other); err == nil {
		t.Fatal("accepted identity collision")
	}
	ctx, cancel := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
	defer cancel()
	if _, err := e.Step(ctx, name, m); err == nil {
		t.Fatal("ignored deadline")
	}
	key := e.Base(m) + "/bad"
	s.objects[key] = []byte("other")
	s.versions[key] = 1
	if err := e.immutable(context.Background(), key, []byte("value")); err == nil {
		t.Fatal("accepted immutable collision")
	}
}
func TestEmptyAndSymlink(t *testing.T) {
	e, _, name, m := fixture(t)
	_ = os.Truncate(name, 0)
	m.Sealed = true
	index, err := e.Step(context.Background(), name, m)
	if err != nil || !index.Complete || index.Bytes != 0 {
		t.Fatal(err)
	}
	link := name + "-link"
	_ = os.Symlink(name, link)
	if _, err = e.Step(context.Background(), link, m); err == nil {
		t.Fatal("followed symlink")
	}
}

func TestTinyActiveChunksCompactOnceAfterClosure(t *testing.T) {
	e, s, name, m := fixture(t)
	var index Index
	for i := 0; i < 30; i++ {
		f, _ := os.OpenFile(name, os.O_APPEND|os.O_WRONLY, 0600)
		_, _ = f.WriteString("tiny\n")
		_ = f.Close()
		var err error
		index, err = e.Step(context.Background(), name, m)
		if err != nil {
			t.Fatal(err)
		}
	}
	original, _ := os.ReadFile(name)
	m.Sealed = true
	for i := 0; i < 3; i++ {
		var err error
		index, err = e.Step(context.Background(), name, m)
		if err != nil {
			t.Fatal(err)
		}
		if index.Complete {
			break
		}
	}
	if !index.Complete || index.Parts != 1 {
		t.Fatalf("not compacted: %+v", index)
	}
	if string(decodeAll(t, e, s, m, index)) != string(original) {
		t.Fatal("compaction changed bytes")
	}
}

func TestCommittedResponseLossResumesWithoutDuplicateBytes(t *testing.T) {
	e, s, name, m := fixture(t)
	if _, err := e.Step(context.Background(), name, m); err != nil {
		t.Fatal(err)
	}
	f, _ := os.OpenFile(name, os.O_APPEND|os.O_WRONLY, 0600)
	_, _ = f.WriteString("second\n")
	_ = f.Close()
	s.lostResponse = true
	if _, err := e.Step(context.Background(), name, m); err == nil {
		t.Fatal("expected lost commit response")
	}
	m.Sealed = true
	index, err := e.Step(context.Background(), name, m)
	if err != nil || !index.Complete || string(decodeAll(t, e, s, m, index)) != "first\nsecond\n" {
		t.Fatalf("%+v %v", index, err)
	}
}

func TestCompleteSourceReplacementAndSameSizeRewriteInvalidateIndex(t *testing.T) {
	for _, mode := range []string{"inode", "rewrite", "handshake"} {
		t.Run(mode, func(t *testing.T) {
			e, s, name, m := fixture(t)
			m.Sealed = true
			if _, err := e.Step(context.Background(), name, m); err != nil {
				t.Fatal(err)
			}
			switch mode {
			case "inode":
				_ = os.Rename(name, name+".old")
				_ = os.WriteFile(name, []byte("first\n"), 0600)
			case "rewrite":
				_ = os.WriteFile(name, []byte("other\n"), 0600)
				future := time.Now().Add(time.Second)
				_ = os.Chtimes(name, future, future)
			case "handshake":
				m.Invalid = true
			}
			if _, err := e.Step(context.Background(), name, m); err == nil {
				t.Fatal("accepted changed source")
			}
			var index Index
			_ = json.Unmarshal(s.objects[e.Base(m)+"/index.json"], &index)
			if index.Error == "" {
				t.Fatal("left complete index readable")
			}
		})
	}
}

func TestMultiBlockCompactionSurvivesRestart(t *testing.T) {
	e, s, name, m := fixture(t)
	for i := 0; i < 20; i++ {
		f, _ := os.OpenFile(name, os.O_APPEND|os.O_WRONLY, 0600)
		_, _ = f.WriteString(strings.Repeat("x", 100000))
		_ = f.Close()
		if _, err := e.Step(context.Background(), name, m); err != nil {
			t.Fatal(err)
		}
	}
	m.Sealed = true
	index, err := e.Step(context.Background(), name, m)
	if err != nil || index.Complete || !index.Finalizing || index.CompactBytes != BlockSize {
		t.Fatalf("%+v %v", index, err)
	}
	restarted := &Engine{Store: s, Prefix: e.Prefix}
	index, err = restarted.Step(context.Background(), name, m)
	original, _ := os.ReadFile(name)
	if err != nil || !index.Complete || string(decodeAll(t, e, s, m, index)) != string(original) {
		t.Fatalf("%+v %v", index, err)
	}
}

// A Go-produced wire fixture is also read by the Python reader tests.
func TestExportWireFixture(t *testing.T) {
	output := os.Getenv("TASK78_WIRE_FIXTURE")
	if output == "" {
		t.Skip("fixture export only")
	}
	e, s, name, m := fixture(t)
	m.Sealed = true
	e.Now = func() time.Time { return time.Unix(4000000000, 0) }
	if _, err := e.Step(context.Background(), name, m); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.MarshalIndent(s.objects, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(output, encoded, 0600); err != nil {
		t.Fatal(err)
	}
}
