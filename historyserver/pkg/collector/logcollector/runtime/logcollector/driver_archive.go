package logcollector

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/ray-project/kuberay/historyserver/pkg/collector/driverarchive"
	"github.com/ray-project/kuberay/historyserver/pkg/utils"
	"github.com/sirupsen/logrus"
)

const markerDirectory = ".driver-archive-v2"

func (r *RayLogHandler) driverEngine() (*driverarchive.Engine, error) {
	store, ok := r.Writer.(driverarchive.Store)
	if !ok {
		return nil, errors.New("driver archive v2 requires a conditional S3 store")
	}
	return &driverarchive.Engine{Store: store, Prefix: r.ClusterDir}, nil
}
func loadDriverMarker(filename string) (driverarchive.Marker, error) {
	var m driverarchive.Marker
	fd, err := syscall.Open(filename, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if err != nil {
		return m, err
	}
	f := os.NewFile(uintptr(fd), filename)
	if err != nil {
		return m, err
	}
	defer f.Close()
	b, err := io.ReadAll(io.LimitReader(f, driverarchive.MetadataLimit+1))
	if err != nil {
		return m, err
	}
	if len(b) > driverarchive.MetadataLimit {
		return m, errors.New("driver marker exceeds limit")
	}
	if err = json.Unmarshal(b, &m); err != nil {
		return m, err
	}
	return m, m.Validate()
}
func driverMarkerPath(filename string) string {
	base := filepath.Base(filename)
	if !strings.HasPrefix(base, "job-driver-") || !strings.HasSuffix(base, ".log") {
		return ""
	}
	submission := strings.TrimSuffix(strings.TrimPrefix(base, "job-driver-"), ".log")
	return filepath.Join(filepath.Dir(filename), markerDirectory, submission+".json")
}
func (r *RayLogHandler) managedDriver(filename string) bool {
	// Once opted in, a missing or corrupt handshake must not let an unsealed
	// driver snapshot masquerade as a complete legacy archive.
	return r.DriverArchiveEnabled && driverMarkerPath(filename) != ""
}
func (r *RayLogHandler) flushDriver(filename string) error {
	parent := r.driverContext
	if parent == nil {
		parent = context.Background()
	}
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	return r.flushDriverContext(ctx, filename)
}

func (r *RayLogHandler) flushDriverContext(ctx context.Context, filename string) error {
	m, err := loadDriverMarker(driverMarkerPath(filename))
	if err != nil {
		return err
	}
	engine, err := r.driverEngine()
	if err != nil {
		return err
	}
	for ctx.Err() == nil {
		index, err := engine.Step(ctx, filename, m)
		if err != nil {
			return err
		}
		if index.Complete {
			return nil
		}
		if index.Bytes == index.Observed && !index.Finalizing {
			return errors.New("driver writer still open; archive is incomplete")
		}
	}
	return ctx.Err()
}

// One serial worker, four <=1MiB chunks per tick, no goroutine per file. Each
// directory iterator advances across ticks so one large directory cannot starve
// later files. No network request is made when a completed file is unchanged.
func (r *RayLogHandler) runDriverArchive(stop <-chan struct{}, done chan<- struct{}) {
	defer close(done)
	engine, err := r.driverEngine()
	if err != nil {
		logrus.Error(err)
		return
	}
	cursors := map[string]*os.File{}
	defer func() {
		for _, f := range cursors {
			_ = f.Close()
		}
	}()
	type remembered struct {
		size         int64
		modified     int64
		completed    bool
		marker       driverarchive.Marker
		device       uint64
		inode        uint64
		next         time.Time
		failures     int
		confirmed    int64
		pendingSince time.Time
	}
	states := map[string]remembered{}
	parent, stopScan := context.WithCancel(context.Background())
	defer stopScan()
	go func() {
		select {
		case <-stop:
			stopScan()
		case <-parent.Done():
		}
	}()
	directoryStart := 0
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	scan := func() {
		ctx, cancel := context.WithTimeout(parent, 20*time.Second)
		defer cancel()
		if !r.lockPrevContext(ctx) {
			return
		}
		defer r.prevProcessMu.Unlock()
		session, err := filepath.EvalSymlinks(utils.GetRaySessionLatestPath())
		if err != nil {
			return
		}
		dirs := []string{filepath.Join(session, "logs", markerDirectory)}
		previous, _ := filepath.Glob(filepath.Join(r.prevLogsDir, "*", "*", "logs", markerDirectory))
		dirs = append(dirs, previous...)
		if len(dirs) > 256 {
			logrus.Error("driver archive directory limit reached")
			dirs = dirs[:256]
		}
		if len(dirs) > 0 {
			directoryStart %= len(dirs)
			dirs = append(dirs[directoryStart:], dirs[:directoryStart]...)
			directoryStart++
		}
		active := map[string]bool{}
		for _, d := range dirs {
			active[d] = true
		}
		for d, f := range cursors {
			if !active[d] {
				_ = f.Close()
				delete(cursors, d)
			}
		}
		for marker := range states {
			if !active[filepath.Dir(marker)] {
				delete(states, marker)
			}
		}
		uploads, examined, failures := 0, 0, 0
		for _, dir := range dirs {
			f := cursors[dir]
			if f == nil {
				f, err = os.Open(dir)
				if errors.Is(err, os.ErrNotExist) {
					continue
				}
				if err != nil {
					logrus.Error(err)
					continue
				}
				cursors[dir] = f
			}
			for examined < 512 && uploads < 4 && ctx.Err() == nil {
				select {
				case <-stop:
					return
				default:
				}
				entries, readErr := f.ReadDir(1)
				if len(entries) == 0 {
					_ = f.Close()
					delete(cursors, dir)
					if readErr != nil && !errors.Is(readErr, io.EOF) {
						logrus.Error(readErr)
					}
					break
				}
				entry := entries[0]
				examined++
				if !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), ".json") {
					continue
				}
				markerPath := filepath.Join(dir, entry.Name())
				m, err := loadDriverMarker(markerPath)
				if err != nil {
					failures++
					logrus.WithError(err).Error("invalid driver archive marker")
					continue
				}
				if entry.Name() != m.Submission+".json" {
					failures++
					logrus.Error("driver marker filename mismatch")
					continue
				}
				filename := filepath.Join(filepath.Dir(dir), "job-driver-"+m.Submission+".log")
				stat, err := os.Stat(filename)
				if err != nil {
					failures++
					logrus.WithError(err).Warn("driver marker source unavailable")
					continue
				}
				state := states[markerPath]
				sys, ok := stat.Sys().(*syscall.Stat_t)
				if !ok {
					continue
				}
				if time.Now().Before(state.next) || (state.completed && m.Sealed && state.marker == m && state.device == uint64(sys.Dev) && state.inode == sys.Ino && state.size == stat.Size() && state.modified == stat.ModTime().UnixNano()) {
					continue
				}
				pendingSince := state.pendingSince
				if pendingSince.IsZero() {
					pendingSince = time.Now()
				}
				uploads++
				index, err := engine.Step(ctx, filename, m)
				if err != nil {
					failures++
					state.failures = min(state.failures+1, 6)
					state.next = time.Now().Add(time.Duration(1<<state.failures)*time.Second + time.Duration(rand.IntN(1000))*time.Millisecond)
					logrus.WithError(err).WithField("submission", m.Submission).Error("driver archive upload pending")
				} else {
					state = remembered{size: stat.Size(), modified: stat.ModTime().UnixNano(), completed: index.Complete, confirmed: index.Bytes, marker: m, device: uint64(sys.Dev), inode: sys.Ino}
				}
				state.size = stat.Size()
				if !state.completed || err != nil {
					state.pendingSince = pendingSince
					state.completed = false
				}
				if len(states) < 4096 || states[markerPath].modified != 0 {
					states[markerPath] = state
				}
			}
		}
		var pendingBytes int64
		var pendingFiles int
		var oldest float64
		for _, state := range states {
			if !state.completed {
				pendingFiles++
				pendingBytes += max(int64(0), state.size-state.confirmed)
				if !state.pendingSince.IsZero() {
					oldest = max(oldest, time.Since(state.pendingSince).Seconds())
				}
			}
		}
		logrus.WithFields(logrus.Fields{"driver_archive_tracked_pending_bytes": pendingBytes, "driver_archive_tracked_pending_files": pendingFiles, "driver_archive_oldest_tracked_pending_seconds": oldest, "driver_archive_tracking_limit_reached": len(states) >= 4096, "driver_archive_examined": examined, "driver_archive_attempted": uploads, "driver_archive_failed": failures}).Info("driver archive scan")
	}
	scan()
	for {
		select {
		case <-stop:
			return
		case <-ticker.C:
			scan()
		}
	}
}

func (r *RayLogHandler) validateDriverArchive() error {
	if !r.DriverArchiveEnabled {
		return nil
	}
	if r.ClusterDir == "" {
		return fmt.Errorf("driver archive requires a cluster prefix")
	}
	_, err := r.driverEngine()
	return err
}

// Stop waiting for legacy traversal/moves when the shared shutdown budget ends.
func (r *RayLogHandler) lockPrevContext(ctx context.Context) bool {
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for ctx.Err() == nil {
		if r.prevProcessMu.TryLock() {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-ticker.C:
		}
	}
	return false
}

// One overall deadline covers every driver file, including lock acquisition.
// Legacy/system logs are processed only after this priority pass.
func (r *RayLogHandler) flushAllDrivers(ctx context.Context) {
	if !r.DriverArchiveEnabled || !r.lockPrevContext(ctx) {
		return
	}
	defer r.prevProcessMu.Unlock()
	dirs := []string{filepath.Join(utils.GetRaySessionLatestPath(), "logs", markerDirectory)}
	previous, _ := filepath.Glob(filepath.Join(r.prevLogsDir, "*", "*", "logs", markerDirectory))
	dirs = append(dirs, previous...)
	for _, dir := range dirs {
		if ctx.Err() != nil {
			break
		}
		f, err := os.Open(dir)
		if err != nil {
			continue
		}
		for ctx.Err() == nil {
			entries, err := f.ReadDir(1)
			if len(entries) == 0 {
				break
			}
			entry := entries[0]
			if !entry.Type().IsRegular() || !strings.HasSuffix(entry.Name(), ".json") {
				continue
			}
			name := filepath.Join(filepath.Dir(dir), "job-driver-"+strings.TrimSuffix(entry.Name(), ".json")+".log")
			if err = r.flushDriverContext(ctx, name); err != nil {
				logrus.WithError(err).WithField("file", filepath.Base(name)).Warn("driver archive final flush incomplete")
			}
		}
		_ = f.Close()
	}
	if ctx.Err() != nil {
		logrus.Warn("driver archive shared shutdown budget exhausted; local sources retained")
	}
}
