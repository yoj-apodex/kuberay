package logcollector

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type failingWriter struct {
	*MockStorageWriter
	fail bool
}

func (w *failingWriter) WriteFile(key string, r io.ReadSeeker) error {
	if w.fail && strings.HasSuffix(key, "failed.log") {
		return errors.New("S3 unavailable")
	}
	return w.MockStorageWriter.WriteFile(key, r)
}
func TestFailedFileSurvivesAndRetriesWithoutDeletingNewFiles(t *testing.T) {
	base := t.TempDir()
	writer := &failingWriter{NewMockStorageWriter(), true}
	h := &RayLogHandler{Writer: writer, RootDir: "root", RayClusterName: "cluster", RayClusterNamespace: "namespace", prevLogsDir: filepath.Join(base, "prev"), persistCompleteLogsDir: filepath.Join(base, "complete")}
	dir := filepath.Join(h.prevLogsDir, "session_test", "node")
	failed := filepath.Join(dir, "logs", "nested", "failed.log")
	passed := filepath.Join(dir, "logs", "ok.log")
	createTestLogFile(t, failed, "must survive")
	createTestLogFile(t, passed, "ok")
	h.processPrevLogsDir(dir)
	if data, err := os.ReadFile(failed); err != nil || string(data) != "must survive" {
		t.Fatalf("failed source lost: %v", err)
	}
	if _, err := os.Stat(passed); !os.IsNotExist(err) {
		t.Fatal("successful source not moved")
	}
	writer.fail = false
	h.processPrevLogsDir(dir)
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("empty tree was not cleaned")
	}
}
func TestPersistedFileWithDifferentContentMustNotSuppressUpload(t *testing.T) {
	base := t.TempDir()
	h := &RayLogHandler{prevLogsDir: filepath.Join(base, "prev"), persistCompleteLogsDir: filepath.Join(base, "complete")}
	source := filepath.Join(h.prevLogsDir, "session", "node", "logs", "job.log")
	createTestLogFile(t, source, "new contents")
	createTestLogFile(t, filepath.Join(h.persistCompleteLogsDir, "session", "node", "logs", "job.log"), "old contents")
	if h.isFileAlreadyPersisted(source, "session", "node") {
		t.Fatal("stale file accepted as acknowledgement")
	}
}

func TestDriverShutdownLockWaitUsesOverallDeadline(t *testing.T) {
	h := &RayLogHandler{DriverArchiveEnabled: true}
	h.prevProcessMu.Lock()
	defer h.prevProcessMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	start := time.Now()
	h.flushAllDrivers(ctx)
	if time.Since(start) > 250*time.Millisecond {
		t.Fatal("shutdown waited beyond budget")
	}
}

func TestMarkerSymlinkRejected(t *testing.T) {
	dir := t.TempDir()
	source := filepath.Join(dir, "marker.json")
	createTestLogFile(t, source, "{}")
	link := filepath.Join(dir, "link.json")
	if err := os.Symlink(source, link); err != nil {
		t.Fatal(err)
	}
	if _, err := loadDriverMarker(link); err == nil {
		t.Fatal("followed marker symlink")
	}
}

func TestEnabledDriverWithoutMarkerCannotFallBackToLegacy(t *testing.T) {
	writer := NewMockStorageWriter()
	h := &RayLogHandler{Writer: writer, DriverArchiveEnabled: true}
	dir := t.TempDir()
	name := filepath.Join(dir, "job-driver-missing.log")
	createTestLogFile(t, name, "unsealed tail")
	if !h.managedDriver(name) {
		t.Fatal("missing handshake bypasses v2")
	}
	if err := h.processPrevLogFile(name, dir, "session", "node"); err == nil {
		t.Fatal("accepted unmarked driver")
	}
	if _, err := os.Stat(name); err != nil {
		t.Fatal("lost source")
	}
}
