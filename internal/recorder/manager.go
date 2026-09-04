// Package recorder implements the recording state machine: it reconciles the
// schedule against running ffmpeg processes, persists session metadata and
// hands finished recordings to the uploader.
package recorder

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spectado/stream-recorder/internal/adts"
	"github.com/spectado/stream-recorder/internal/config"
	"github.com/spectado/stream-recorder/internal/metrics"
	"github.com/spectado/stream-recorder/internal/schedule"
	"github.com/spectado/stream-recorder/internal/sysmon"
)

// Uploader stores finished recordings and the playlists next to them.
// Implementations must only return nil from Upload once the object is durably
// stored and verified.
type Uploader interface {
	// Upload stores the file at path under key without overwriting a
	// different existing object (see ErrObjectExists).
	Upload(ctx context.Context, path, key, contentType string, metadata map[string]string) (etag string, err error)
	// PutObject stores a small object under key, replacing any existing one.
	PutObject(ctx context.Context, key, contentType string, body []byte) error
	// GetObject returns the content of key; found is false when there is no
	// such object.
	GetObject(ctx context.Context, key string) (body []byte, found bool, err error)
}

// Verifier is optionally implemented by an Uploader: it reports whether an
// object with exactly the given size already exists under key.
type Verifier interface {
	Exists(ctx context.Context, key string, size int64) (bool, error)
}

// ErrObjectExists is returned by an Uploader when the key is already taken by
// a different object (conditional put failed). The manager picks a new key.
var ErrObjectExists = errors.New("object already exists with different content")

// PermanentError marks upload failures that will not go away by retrying
// quickly (bad credentials, missing bucket, rejected request). The manager
// keeps retrying, but slowly, and flags the session as blocked.
type PermanentError struct{ Err error }

func (e *PermanentError) Error() string { return e.Err.Error() }
func (e *PermanentError) Unwrap() error { return e.Err }

// ScheduleInfo describes the state of schedule fetching.
type ScheduleInfo struct {
	URL                 string                 `json:"url"`
	PollInterval        string                 `json:"pollInterval"`
	Loaded              bool                   `json:"loaded"`
	Origin              string                 `json:"origin,omitempty"` // url | cache
	FetchedAt           *time.Time             `json:"fetchedAt,omitempty"`
	LastAttemptAt       *time.Time             `json:"lastAttemptAt,omitempty"`
	LastSuccessAt       *time.Time             `json:"lastSuccessAt,omitempty"`
	LastError           string                 `json:"lastError,omitempty"`
	LastErrorAt         *time.Time             `json:"lastErrorAt,omitempty"`
	ConsecutiveFailures int                    `json:"consecutiveFailures"`
	ItemCount           int                    `json:"itemCount"`
	InvalidItems        []schedule.InvalidItem `json:"invalidItems,omitempty"`
	ETag                string                 `json:"etag,omitempty"`
	ClockSkewSeconds    float64                `json:"clockSkewSeconds"`
}

// Manager owns all recording sessions.
type Manager struct {
	cfg       *config.Config
	log       *slog.Logger
	met       *metrics.Metrics
	up        Uploader // nil when uploads are disabled
	root      string   // <DataDir>/recordings
	version   string
	startedAt time.Time
	caps      FFmpegCapabilities

	ffmpegCount   atomic.Int64
	lastReconcile atomic.Int64 // unix nanos
	probeSem      chan struct{}
	uploadSem     chan struct{}

	mu            sync.Mutex
	sched         *schedule.Schedule
	info          ScheduleInfo
	active        map[string]*activeRecording // item id -> capture in progress
	sessions      map[string]*Session         // session id -> every known session
	uploadLoops   map[string]bool             // session id -> upload goroutine alive
	skipLogged    map[string]time.Time
	loggedInvalid map[string]struct{}
	shuttingDown  atomic.Bool
	schedLoaded   atomic.Bool
	activeCount   atomic.Int64
	diskLow       atomic.Bool
	diskFree      func() (free uint64, ok bool)

	uploadCtx     context.Context
	uploadCancel  context.CancelFunc
	uploadWG      sync.WaitGroup
	stopWG        sync.WaitGroup
	playlistLocks keyedLocks // serialises rewrites of the same index.m3u8
}

// NewManager creates a Manager. up may be nil to keep recordings locally.
func NewManager(cfg *config.Config, log *slog.Logger, met *metrics.Metrics, up Uploader, version string) *Manager {
	uctx, ucancel := context.WithCancel(context.Background())
	m := &Manager{
		cfg:           cfg,
		log:           log.With("component", "recorder"),
		met:           met,
		up:            up,
		root:          filepath.Join(cfg.DataDir, "recordings"),
		version:       version,
		startedAt:     time.Now(),
		probeSem:      make(chan struct{}, cfg.ProbeConcurrency),
		uploadSem:     make(chan struct{}, cfg.UploadConcurrency),
		active:        map[string]*activeRecording{},
		sessions:      map[string]*Session{},
		uploadLoops:   map[string]bool{},
		skipLogged:    map[string]time.Time{},
		loggedInvalid: map[string]struct{}{},
		diskFree:      func() (uint64, bool) { return 0, false },
		uploadCtx:     uctx,
		uploadCancel:  ucancel,
	}
	m.info.PollInterval = cfg.SchedulePollInterval.String()
	return m
}

// SetFFmpegCapabilities installs the detected ffmpeg capabilities.
func (m *Manager) SetFFmpegCapabilities(c FFmpegCapabilities) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.caps = c
}

// SetDiskFree installs the function used to check free space before starting
// a recording.
func (m *Manager) SetDiskFree(fn func() (uint64, bool)) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if fn != nil {
		m.diskFree = fn
	}
}

// FFmpegProcessCount returns the number of ffmpeg processes currently running.
func (m *Manager) FFmpegProcessCount() int { return int(m.ffmpegCount.Load()) }

// Ready reports whether the recorder can accept work: a schedule is loaded,
// it is not shutting down and the disk is not low.
func (m *Manager) Ready() (bool, string) {
	switch {
	case m.shuttingDown.Load():
		return false, "shutting down"
	case !m.schedLoaded.Load():
		return false, "no schedule loaded yet"
	case m.diskLow.Load():
		return false, "free disk below MIN_FREE_DISK"
	}
	return true, ""
}

// Health reports internal liveness: the reconcile loop must keep ticking.
func (m *Manager) Health() (bool, map[string]any) {
	ok := true
	checks := map[string]any{}
	last := m.lastReconcile.Load()
	switch {
	case last == 0 && time.Since(m.startedAt) < 30*time.Second:
		checks["reconcile"] = "starting"
	case last == 0 || time.Since(time.Unix(0, last)) > 15*time.Second:
		ok = false
		checks["reconcile"] = "stalled"
	default:
		checks["reconcile"] = "ok"
	}
	checks["shuttingDown"] = m.shuttingDown.Load()
	checks["activeRecordings"] = m.activeCount.Load()
	checks["ffmpegProcesses"] = m.FFmpegProcessCount()
	return ok, checks
}

// applyScheduleLocked installs a new last-known-good schedule.
func (m *Manager) applyScheduleLocked(s *schedule.Schedule) {
	m.sched = s
	m.schedLoaded.Store(true)
	m.info.Loaded = true
	m.info.Origin = s.Origin
	if !s.FetchedAt.IsZero() {
		t := s.FetchedAt
		m.info.FetchedAt = &t
	}
	m.info.ItemCount = len(s.Items)
	m.info.InvalidItems = s.Invalid
	m.info.ETag = s.ETag
	if !s.ServerDate.IsZero() && !s.FetchedAt.IsZero() {
		skew := s.FetchedAt.Sub(s.ServerDate).Seconds()
		m.info.ClockSkewSeconds = float64(int(skew*10)) / 10
		m.met.ScheduleClockSkew.Set(skew)
		if skew > 5 || skew < -5 {
			m.log.Warn("clock skew between recorder and schedule server", "skewSeconds", skew)
		}
	}
	m.met.ScheduleLoaded.Set(1)
	m.met.ScheduleItems.Set(float64(len(s.Items)))
	m.met.ScheduleInvalidItems.Set(float64(len(s.Invalid)))
	// Log rejected items only when the set of rejections changes, otherwise a
	// permanently broken entry would be reported on every poll.
	seen := make(map[string]struct{}, len(s.Invalid))
	for _, inv := range s.Invalid {
		key := fmt.Sprintf("%d|%s|%s", inv.Index, inv.ID, inv.Reason)
		seen[key] = struct{}{}
		if _, known := m.loggedInvalid[key]; !known {
			m.log.Warn("schedule item rejected", "index", inv.Index, "id", inv.ID, "reason", inv.Reason)
		}
	}
	m.loggedInvalid = seen
}

// SetSchedule installs a schedule (used by tests and the poller) and reconciles.
func (m *Manager) SetSchedule(s *schedule.Schedule) {
	m.mu.Lock()
	m.applyScheduleLocked(s)
	m.mu.Unlock()
	m.Reconcile(time.Now())
}

// ---------------------------------------------------------------------------
// Reconcile
// ---------------------------------------------------------------------------

// RunReconcile evaluates the schedule once per second until ctx is done.
func (m *Manager) RunReconcile(ctx context.Context) {
	m.Reconcile(time.Now())
	t := time.NewTicker(time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			m.Reconcile(now)
		}
	}
}

func (m *Manager) windowFor(it schedule.Item) (start, end time.Time) {
	early, late := m.cfg.RecordStartEarly, m.cfg.RecordStopLate
	if it.StartEarly != nil {
		early = it.StartEarly.D()
	}
	if it.StopLate != nil {
		late = it.StopLate.D()
	}
	return it.Start.Add(-early), it.End.Add(late)
}

// coveredLocked reports whether a finished session already recorded this
// item's window: it ended normally (scheduled end reached) with an end no
// earlier than the item's current end. Sessions stopped because the item was
// removed, rotated or the process shut down never cover, so a re-added item
// records again and an extended end starts a new part.
func (m *Manager) coveredLocked(it schedule.Item) bool {
	for _, s := range m.sessions {
		if s.ID == it.ID && s.FinishReason == ReasonEnded && s.SessionEnd != nil && !s.End.Before(it.End) {
			return true
		}
	}
	return false
}

// Reconcile applies the rules:
//   - an active recording stops only when the latest known end (+stop-late)
//     has passed or its item disappeared from a successfully loaded schedule
//     (an item that merely became invalid keeps recording with its last good
//     definition);
//   - any schedule item inside its window that is not recording, and not
//     already covered by a normally finished session, is started.
func (m *Manager) Reconcile(now time.Time) {
	m.mu.Lock()
	defer m.mu.Unlock()
	// Stamped after acquiring the lock so lock starvation shows up in /healthz.
	m.lastReconcile.Store(time.Now().UnixNano())
	if m.shuttingDown.Load() {
		return
	}
	sched := m.sched

	// Disk state (evaluated once per tick).
	if free, ok := m.diskFree(); ok {
		low := free < uint64(m.cfg.MinFreeDiskBytes)
		if low != m.diskLow.Load() {
			if low {
				m.log.Error("free disk below MIN_FREE_DISK; new recordings will not start", "free", free, "min", m.cfg.MinFreeDiskBytes)
			} else {
				m.log.Info("free disk recovered above MIN_FREE_DISK", "free", free)
			}
		}
		m.diskLow.Store(low)
		if low {
			m.met.DiskLow.Set(1)
		} else {
			m.met.DiskLow.Set(0)
		}
	}

	// 1. Active recordings.
	for id, ar := range m.active {
		if ar.stopping {
			continue
		}
		present := true
		if sched != nil {
			if it, ok := sched.Get(id); ok {
				if ar.s.applyItem(it) {
					m.log.Info("source changed for active recording; applied on next ffmpeg restart", "id", id, "source", RedactURL(it.Source))
				}
				ar.s.ScheduleNote = ""
			} else if sched.Present(id) {
				if ar.s.ScheduleNote == "" {
					reason := sched.InvalidReason(id)
					ar.s.ScheduleNote = fmt.Sprintf("item invalid in schedule since %s (%s); recording continues with last valid definition",
						now.UTC().Format(time.RFC3339), reason)
					m.log.Warn("active recording's schedule item became invalid; keeping last valid definition", "id", id, "reason", reason)
				}
			} else {
				present = false
			}
		}
		stopLate := ar.s.effectiveStopLate(m.cfg.RecordStopLate)
		switch {
		case !present:
			m.stopLocked(ar, ReasonRemoved)
		case !now.Before(ar.s.End.Add(stopLate)):
			m.stopLocked(ar, ReasonEnded)
		case m.cfg.MaxSessionDuration > 0 && now.Sub(ar.s.SessionStart) >= m.cfg.MaxSessionDuration:
			m.stopLocked(ar, ReasonRotated)
		case !ar.supervised:
			m.startSupervisorLocked(ar)
		}
	}

	// 2. Items that should be recording.
	if sched != nil {
		for _, it := range sched.Items {
			if _, ok := m.active[it.ID]; ok {
				continue
			}
			start, end := m.windowFor(it)
			if now.Before(start) || !now.Before(end) {
				continue
			}
			if m.coveredLocked(it) {
				continue
			}
			if m.cfg.MaxRecordings > 0 && len(m.active) >= m.cfg.MaxRecordings {
				m.skipLocked(it.ID, "max_recordings", fmt.Sprintf("MAX_RECORDINGS=%d reached", m.cfg.MaxRecordings))
				continue
			}
			if m.diskLow.Load() {
				m.skipLocked(it.ID, "disk_low", fmt.Sprintf("free disk below MIN_FREE_DISK (%d bytes)", m.cfg.MinFreeDiskBytes))
				continue
			}
			if err := m.startSessionLocked(it, now); err != nil {
				m.skipLocked(it.ID, "error", err.Error())
			}
		}
	}
	m.met.RecordingsActive.Set(float64(len(m.active)))
	m.activeCount.Store(int64(len(m.active)))
}

func (m *Manager) skipLocked(id, reason, detail string) {
	m.met.RecordingsSkippedTotal.WithLabelValues(reason).Inc()
	key := reason + "|" + id
	if last, ok := m.skipLogged[key]; ok && time.Since(last) < 5*time.Minute {
		return
	}
	m.skipLogged[key] = time.Now()
	m.log.Error("cannot start recording", "id", id, "reason", reason, "detail", detail)
}

// ---------------------------------------------------------------------------
// Session lifecycle
// ---------------------------------------------------------------------------

func (m *Manager) openActive(s *Session, resume bool) (*activeRecording, error) {
	if err := os.MkdirAll(s.dir, 0o755); err != nil {
		return nil, fmt.Errorf("create session dir: %w", err)
	}
	if resume {
		if removed, err := adts.TrimPartialTail(s.FilePath()); err != nil {
			if !errors.Is(err, os.ErrNotExist) {
				m.log.Warn("could not check recording tail", "id", s.ID, "file", s.FilePath(), "error", err)
			}
		} else if removed > 0 {
			s.TrimmedBytes += removed
			m.log.Info("trimmed partial ADTS frame before resuming", "id", s.ID, "session", s.SessionID, "bytes", removed)
		}
	}
	f, err := os.OpenFile(s.FilePath(), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("open recording file: %w", err)
	}
	var size int64
	if st, err := f.Stat(); err == nil {
		size = st.Size()
	}
	ctx, cancel := context.WithCancel(context.Background())
	id := s.ID
	logger := m.log.With("id", id)
	ar := &activeRecording{
		s:      s,
		file:   f,
		ctx:    ctx,
		cancel: cancel,
		done:   make(chan struct{}),
		stderr: newStderrBuffer(m.cfg.FFmpegStderrLog, func(level slog.Level, line string) {
			logger.Log(context.Background(), level, line, "source", "ffmpeg")
		}),
		bytesCounter:    m.met.RecordingBytes.WithLabelValues(id),
		runningGauge:    m.met.RecordingFFmpegRunning.WithLabelValues(id),
		lastDataGauge:   m.met.RecordingLastData.WithLabelValues(id),
		restartsCounter: m.met.FFmpegRestartsTotal.WithLabelValues(id),
	}
	ar.bytes.Store(size)
	ar.savedBytes = size
	ar.runningGauge.Set(0)
	return ar, nil
}

func (m *Manager) startSessionLocked(it schedule.Item, now time.Time) error {
	safe := schedule.SafeID(it.ID)
	stamp := now.UTC().Format("20060102T150405Z")
	base := safe + "_" + stamp
	sid := base
	for i := 2; m.sessions[sid] != nil; i++ {
		sid = base + "-" + strconv.Itoa(i)
	}
	s := &Session{
		ID:           it.ID,
		SafeID:       safe,
		SessionID:    sid,
		SessionStart: now,
		State:        StateRecording,
		Codec:        "auto",
		dir:          filepath.Join(m.root, safe),
	}
	s.applyItem(it)
	// Recurring shows: reuse the codec decision of the most recent session with
	// the same id and source so a mass start does not queue behind ffprobe.
	if s.Codec == "" || s.Codec == "auto" {
		var latest time.Time
		for _, other := range m.sessions {
			if other.ID == it.ID && other.Source == it.Source && other.ResolvedCodec != "" && other.SessionStart.After(latest) {
				latest = other.SessionStart
				s.ResolvedCodec = other.ResolvedCodec
			}
		}
	}
	// Every session gets its own file inside the recording's folder; later
	// parts of the same show (rotation, extension after the end) land next to
	// the first one and are added to the folder's index.m3u8.
	folder := defaultFolder(m.cfg.S3Prefix, s)
	if it.Key != "" {
		// An explicit key names the folder.
		s.KeyTemplate = it.Key
		folder = m.cfg.S3Prefix + strings.Trim(it.Key, "/") + "/"
	}
	s.Key = mediaKey(folder, s)

	// Sidecar first: a crash between the two steps leaves metadata without a
	// file (harmless) rather than an orphan file without metadata. No fsync
	// here (mass starts would serialise hundreds of them under the lock); the
	// persister rewrites it within 10 s and transitions fsync.
	if err := s.saveQuick(); err != nil {
		return fmt.Errorf("write sidecar: %w", err)
	}
	ar, err := m.openActive(s, false)
	if err != nil {
		_ = os.Remove(s.SidecarPath())
		return err
	}
	m.sessions[sid] = s
	m.active[it.ID] = ar
	m.startSupervisorLocked(ar)
	m.met.RecordingsStartedTotal.Inc()
	m.log.Info("recording started", "id", it.ID, "name", it.Name, "session", sid, "source", RedactURL(it.Source),
		"start", it.Start.UTC().Format(time.RFC3339), "end", it.End.UTC().Format(time.RFC3339), "key", s.Key)
	return nil
}

func (m *Manager) startSupervisorLocked(ar *activeRecording) {
	if ar.supervised {
		return
	}
	ar.supervised = true
	if ar.recovered {
		m.log.Info("resuming recording", "id", ar.s.ID, "session", ar.s.SessionID, "bytes", ar.bytes.Load(),
			"end", ar.s.End.UTC().Format(time.RFC3339))
	}
	go m.supervise(ar)
}

// stopLocked removes the recording from the active set and finalizes it in
// the background (stopping ffmpeg can take a few seconds).
func (m *Manager) stopLocked(ar *activeRecording, reason string) {
	ar.stopping = true
	delete(m.active, ar.s.ID)
	m.stopWG.Add(1)
	go func() {
		defer m.stopWG.Done()
		ar.cancel()
		if ar.supervised {
			<-ar.done
		}
		m.finalize(ar, reason)
	}()
}

// finalize closes the file and either parks the session for resume (shutdown)
// or hands it to the uploader.
func (m *Manager) finalize(ar *activeRecording, reason string) {
	if err := ar.file.Sync(); err != nil {
		m.log.Warn("fsync recording", "id", ar.s.ID, "error", err)
	}
	_ = ar.file.Close()
	now := time.Now()

	m.mu.Lock()
	s := ar.s
	s.Bytes = ar.bytes.Load()
	ar.runningGauge.Set(0)
	m.met.FFmpegRSS.WithLabelValues(s.ID).Set(0)
	if reason == ReasonShutdown {
		s.State = StateRecording // resumed on next start
		s.SuspendedAt = &now
		if err := s.save(); err != nil {
			m.log.Error("save sidecar", "id", s.ID, "error", err)
		}
		m.mu.Unlock()
		m.met.RecordingsSuspendedTotal.Inc()
		m.log.Info("recording suspended for shutdown (resumes on next start)", "id", s.ID, "session", s.SessionID, "bytes", s.Bytes)
		return
	}
	s.SessionEnd = &now
	s.FinishReason = reason
	s.State = StateFinalized
	if st, err := os.Stat(s.FilePath()); err == nil {
		s.Size = st.Size()
	}
	if err := s.save(); err != nil {
		m.log.Error("save sidecar", "id", s.ID, "error", err)
	}
	m.met.RecordingsActive.Set(float64(len(m.active)))
	m.activeCount.Store(int64(len(m.active)))
	m.mu.Unlock()

	m.met.RecordingsFinishedTotal.WithLabelValues(reason).Inc()
	m.log.Info("recording finished", "id", s.ID, "session", s.SessionID, "reason", reason,
		"bytes", s.Bytes, "duration", now.Sub(s.SessionStart).Truncate(time.Second).String(), "restarts", s.Restarts)
	m.enqueueUpload(s)
}

// ---------------------------------------------------------------------------
// Recovery
// ---------------------------------------------------------------------------

// Recover scans the data directory for sessions left behind by a previous
// run: unfinished captures are re-opened for resume (the sidecar itself is
// the last known state, no schedule needed), finished ones are queued for
// upload.
func (m *Manager) Recover() {
	sessions, errs := scanSessions(m.root)
	for _, err := range errs {
		m.log.Warn("scanning recordings", "error", err)
	}
	var toUpload []*Session

	m.mu.Lock()
	for _, s := range sessions {
		m.sessions[s.SessionID] = s
		switch s.State {
		case StateRecording:
			if prev, dup := m.active[s.ID]; dup {
				// Two unfinished sessions for one id: keep the newest, finalize the older.
				delete(m.active, s.ID)
				_ = prev.file.Close()
				prev.s.State = StateFinalized
				prev.s.FinishReason = ReasonSuperseded
				t := time.Now()
				prev.s.SessionEnd = &t
				_ = prev.s.save()
				toUpload = append(toUpload, prev.s)
			}
			s.SessionEnd = nil
			s.SuspendedAt = nil
			ar, err := m.openActive(s, true)
			if err != nil {
				m.log.Error("cannot reopen recording, finalizing instead", "id", s.ID, "session", s.SessionID, "error", err)
				s.State = StateFinalized
				s.FinishReason = ReasonError
				s.LastError = err.Error()
				t := time.Now()
				s.SessionEnd = &t
				_ = s.save()
				toUpload = append(toUpload, s)
				continue
			}
			ar.recovered = true
			m.active[s.ID] = ar
			m.log.Info("recovered unfinished recording", "id", s.ID, "session", s.SessionID, "bytes", ar.bytes.Load(),
				"end", s.End.UTC().Format(time.RFC3339))
		case StateFinalized, StateUploading:
			s.State = StateFinalized
			toUpload = append(toUpload, s)
			m.log.Info("recovered recording waiting for upload", "id", s.ID, "session", s.SessionID, "bytes", s.Bytes)
		case StateUploaded:
			if _, err := os.Stat(s.FilePath()); err == nil {
				// Crash between "uploaded" and the local delete: re-run the upload;
				// the conditional put + size check converges without a second copy.
				m.log.Warn("uploaded recording still has its local file; re-verifying", "id", s.ID, "session", s.SessionID)
				s.State = StateFinalized
				toUpload = append(toUpload, s)
			}
		default:
			// terminal: only kept for visibility until retention removes it
		}
	}
	m.met.RecordingsActive.Set(float64(len(m.active)))
	m.activeCount.Store(int64(len(m.active)))
	m.mu.Unlock()

	m.warnOrphans()
	for _, s := range toUpload {
		m.enqueueUpload(s)
	}
}

// warnOrphans logs audio files that have no sidecar (nothing is deleted).
func (m *Manager) warnOrphans() {
	entries, err := os.ReadDir(m.root)
	if err != nil {
		return
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		files, err := os.ReadDir(filepath.Join(m.root, e.Name()))
		if err != nil {
			continue
		}
		for _, f := range files {
			if strings.HasSuffix(f.Name(), ".aac") {
				sidecar := filepath.Join(m.root, e.Name(), strings.TrimSuffix(f.Name(), ".aac")+".json")
				if _, err := os.Stat(sidecar); err != nil {
					m.log.Warn("orphan recording without metadata (left untouched)", "file", filepath.Join(m.root, e.Name(), f.Name()))
				}
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Upload
// ---------------------------------------------------------------------------

func (m *Manager) enqueueUpload(s *Session) {
	m.mu.Lock()
	if m.shuttingDown.Load() || m.uploadLoops[s.SessionID] {
		m.mu.Unlock()
		return
	}
	if m.up == nil {
		s.State = StateKept
		_ = s.save()
		m.mu.Unlock()
		m.log.Info("uploads disabled, recording kept locally", "id", s.ID, "file", s.FilePath())
		return
	}
	m.uploadLoops[s.SessionID] = true
	m.uploadWG.Add(1)
	m.mu.Unlock()
	go m.uploadLoop(s)
}

var blockedRetryInterval = 15 * time.Minute // var so tests can shorten it

func (m *Manager) uploadLoop(s *Session) {
	defer m.uploadWG.Done()
	ctx := m.uploadCtx
	backoff := 5 * time.Second
	log := m.log.With("id", s.ID, "session", s.SessionID)

	finish := func() {
		m.mu.Lock()
		delete(m.uploadLoops, s.SessionID)
		m.mu.Unlock()
	}
	for {
		select {
		case m.uploadSem <- struct{}{}:
		case <-ctx.Done():
			finish()
			return
		}
		m.met.UploadsInProgress.Inc()
		err := m.uploadOnce(ctx, s)
		m.met.UploadsInProgress.Dec()
		<-m.uploadSem

		if err == nil {
			finish()
			return
		}
		if ctx.Err() != nil {
			m.mu.Lock()
			if s.State == StateUploading {
				s.State = StateFinalized
				_ = s.save()
			}
			m.mu.Unlock()
			finish()
			return
		}
		if errors.Is(err, ErrObjectExists) {
			m.mu.Lock()
			if s.KeyRenames < 3 {
				old := s.Key
				s.KeyRenames++
				s.Key = withSuffix(s.Key, "-"+strconv.Itoa(s.KeyRenames+1))
				s.UploadAttempts++
				s.State = StateFinalized
				_ = s.save()
				m.mu.Unlock()
				log.Warn("object key already taken by a different object; using a new key", "oldKey", old, "newKey", s.Key)
				continue
			}
			renames := s.KeyRenames
			m.mu.Unlock()
			err = &PermanentError{Err: fmt.Errorf("object key still conflicts after %d renames: %w", renames, err)}
		}

		var perm *PermanentError
		blocked := errors.As(err, &perm)
		wait := backoff
		if blocked {
			wait = blockedRetryInterval
		}
		next := time.Now().Add(wait)
		m.mu.Lock()
		s.State = StateFinalized
		s.LastError = "upload: " + err.Error()
		s.UploadAttempts++
		s.UploadBlocked = blocked
		s.NextUploadAt = &next
		attempts := s.UploadAttempts
		_ = s.save()
		m.mu.Unlock()
		m.met.UploadsTotal.WithLabelValues("failure").Inc()
		m.met.UploadRetriesTotal.Inc()
		if blocked {
			log.Error("upload failed with a permanent-looking error (credentials/bucket/request); will retry slowly",
				"error", err, "attempt", attempts, "retryIn", wait.String())
		} else {
			log.Warn("upload failed, will retry", "error", err, "attempt", attempts, "retryIn", wait.String())
		}

		select {
		case <-ctx.Done():
			finish()
			return
		case <-time.After(wait):
		}
		if !blocked {
			backoff *= 2
			if backoff > m.cfg.UploadBackoffMax {
				backoff = m.cfg.UploadBackoffMax
			}
		}
	}
}

// uploadOnce performs one upload attempt: the audio file (skipped when an
// earlier attempt already stored it), then the folder's index.m3u8. A nil
// return means the session reached a terminal state (uploaded, or failed for
// a non-retryable reason).
func (m *Manager) uploadOnce(ctx context.Context, s *Session) error {
	m.mu.Lock()
	path, key := s.FilePath(), s.Key
	st, err := os.Stat(path)
	if err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			m.mu.Unlock()
			return err
		}
		size, stored := s.Size, s.MediaUploaded
		m.mu.Unlock()
		// The file may be gone because a previous run uploaded it and crashed
		// before recording that; ask the store before declaring data lost.
		if !stored {
			if v, ok := m.up.(Verifier); ok && key != "" && size > 0 {
				exists, verr := v.Exists(ctx, key, size)
				if verr != nil {
					return fmt.Errorf("recording file missing; could not verify remote object: %w", verr)
				}
				stored = exists
			}
		}
		if !stored {
			m.mu.Lock()
			s.State = StateFailed
			s.LastError = "recording file missing"
			_ = s.save()
			m.mu.Unlock()
			m.log.Error("recording file missing, cannot upload", "id", s.ID, "file", path)
			return nil
		}
		m.mu.Lock()
		if !s.MediaUploaded {
			m.log.Warn("recording file missing locally but the object exists remotely", "id", s.ID, "key", key)
			s.MediaUploaded = true
		}
		s.State = StateUploading
		_ = s.save()
		m.mu.Unlock()
		return m.completeUpload(ctx, s)
	}
	if st.Size() == 0 {
		s.State = StateFailed
		s.LastError = "empty recording (no data captured)"
		_ = s.save()
		m.mu.Unlock()
		m.log.Error("empty recording, nothing to upload", "id", s.ID, "session", s.SessionID)
		return nil
	}
	size := st.Size()
	s.State = StateUploading
	s.Bytes = size
	s.Size = size
	_ = s.save()
	if s.MediaUploaded {
		// Stored by an earlier attempt; only the playlist is left to do.
		m.mu.Unlock()
		return m.completeUpload(ctx, s)
	}
	m.mu.Unlock()

	// The playlist needs the exact playback time, which only the ADTS frame
	// headers can tell (one sequential read of the file).
	info, err := adts.Scan(path)
	if err != nil {
		return fmt.Errorf("scan recording: %w", err)
	}
	if info.Junk > 0 {
		m.log.Warn("recording contains bytes outside ADTS frames", "id", s.ID, "session", s.SessionID,
			"bytes", info.Junk, "frames", info.Frames)
	}

	m.mu.Lock()
	s.DurationSeconds = info.Duration.Seconds()
	if info.Frames == 0 && s.SessionEnd != nil {
		// Not recognisable as ADTS: fall back to the wall-clock length.
		s.DurationSeconds = s.SessionEnd.Sub(s.SessionStart).Seconds()
	}
	meta := map[string]string{
		"recording-id":     s.ID,
		"session-id":       s.SessionID,
		"name":             s.Name,
		"source":           RedactURL(s.Source),
		"scheduled-start":  s.Start.UTC().Format(time.RFC3339),
		"scheduled-end":    s.End.UTC().Format(time.RFC3339),
		"session-start":    s.SessionStart.UTC().Format(time.RFC3339),
		"duration-seconds": strconv.FormatFloat(s.DurationSeconds, 'f', 3, 64),
		"codec":            s.ResolvedCodec,
		"ffmpeg-restarts":  strconv.Itoa(s.Restarts),
		"finish-reason":    s.FinishReason,
		"recorder-version": m.version,
	}
	if s.SessionEnd != nil {
		meta["session-end"] = s.SessionEnd.UTC().Format(time.RFC3339)
	}
	m.mu.Unlock()

	// Per-attempt deadline: 10 minutes plus the transfer time at the assumed
	// minimum throughput (UPLOAD_MIN_THROUGHPUT). Stalls are bounded separately
	// by the HTTP client's response-header timeout.
	minRate := m.cfg.UploadMinThroughput
	if minRate <= 0 {
		minRate = 128 * 1024
	}
	attempt := 10*time.Minute + time.Duration(size/minRate)*time.Second
	uctx, cancel := context.WithTimeout(ctx, attempt)
	defer cancel()

	m.log.Info("upload started", "id", s.ID, "session", s.SessionID, "key", key, "bytes", size,
		"duration", info.Duration.Truncate(time.Millisecond).String())
	t0 := time.Now()
	etag, err := m.up.Upload(uctx, path, key, "audio/aac", meta)
	if err != nil {
		return err
	}
	dur := time.Since(t0)

	// Remember the stored object BEFORE touching the playlist so a retry never
	// uploads the audio twice. If the sidecar write fails the in-memory flag
	// still skips the re-upload; after a crash the conditional put + size
	// check converge on the same result.
	m.mu.Lock()
	s.MediaUploaded = true
	s.UploadETag = etag
	if err := s.save(); err != nil {
		m.log.Warn("record upload in sidecar", "id", s.ID, "session", s.SessionID, "error", err)
	}
	m.mu.Unlock()
	m.met.UploadBytesTotal.Add(float64(size))
	m.met.UploadDuration.Observe(dur.Seconds())
	m.log.Info("audio uploaded", "id", s.ID, "session", s.SessionID, "key", key, "bytes", size,
		"duration", dur.Truncate(time.Millisecond).String(), "etag", etag)
	return m.completeUpload(ctx, s)
}

// completeUpload publishes the folder's playlist, marks the session uploaded
// and removes the local file (in that order: the file is only deleted once
// the sidecar durably says "uploaded").
func (m *Manager) completeUpload(ctx context.Context, s *Session) error {
	if err := m.publishPlaylist(ctx, s); err != nil {
		return err
	}
	now := time.Now()
	m.mu.Lock()
	path := s.FilePath()
	s.State = StateUploaded
	s.UploadedAt = &now
	s.UploadAttempts++
	s.UploadBlocked = false
	s.LastError = ""
	s.NextUploadAt = nil
	if err := s.save(); err != nil {
		// Never delete the file while the sidecar still says "uploading".
		s.State = StateFinalized
		m.mu.Unlock()
		return fmt.Errorf("record upload in sidecar: %w", err)
	}
	key, size := s.Key, s.Size
	m.mu.Unlock()
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		m.log.Warn("remove uploaded file (will be retried on next start)", "file", path, "error", err)
	}

	m.met.UploadsTotal.WithLabelValues("success").Inc()
	m.log.Info("upload finished", "id", s.ID, "session", s.SessionID, "key", key, "playlist", playlistKey(key), "bytes", size)
	return nil
}

// ---------------------------------------------------------------------------
// Housekeeping
// ---------------------------------------------------------------------------

// RunPersister flushes sidecars/recordings of active sessions, samples the
// ffmpeg children and refreshes gauges every 10 seconds.
func (m *Manager) RunPersister(ctx context.Context) {
	t := time.NewTicker(10 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.persist()
		}
	}
}

func (m *Manager) persist() {
	type sample struct {
		ar  *activeRecording
		pid int
	}
	m.mu.Lock()
	samples := make([]sample, 0, len(m.active))
	for _, ar := range m.active {
		if b := ar.bytes.Load(); b != ar.savedBytes {
			ar.s.Bytes = b
			if err := ar.s.saveQuick(); err != nil {
				m.log.Warn("save sidecar", "id", ar.s.ID, "error", err)
			} else {
				ar.savedBytes = b
			}
		}
		samples = append(samples, sample{ar, int(ar.pid.Load())})
	}
	var onDisk, pendingBytes int64
	var pending, failed, blocked int
	var oldest time.Time
	for _, s := range m.sessions {
		switch s.State {
		case StateRecording, StateFinalized, StateUploading, StateKept:
			onDisk += s.Bytes
		}
		switch s.State {
		case StateFinalized, StateUploading:
			pending++
			pendingBytes += s.Bytes
			if s.UploadBlocked {
				blocked++
			}
			if s.SessionEnd != nil && (oldest.IsZero() || s.SessionEnd.Before(oldest)) {
				oldest = *s.SessionEnd
			}
		case StateFailed:
			failed++
		}
	}
	m.met.RecordingsOnDiskBytes.Set(float64(onDisk))
	m.met.UploadsPending.Set(float64(pending))
	m.met.UploadPendingBytes.Set(float64(pendingBytes))
	m.met.UploadsBlocked.Set(float64(blocked))
	m.met.UploadsFailedFinal.Set(float64(failed))
	if oldest.IsZero() {
		m.met.UploadOldestPendingAge.Set(0)
	} else {
		m.met.UploadOldestPendingAge.Set(time.Since(oldest).Seconds())
	}
	m.mu.Unlock()

	// Slow work outside the lock: fsync the audio files (bounds data loss on
	// power failure to ~10 s) and sample the ffmpeg children via /proc.
	// os.File is goroutine-safe; Sync on a file finalize has closed just
	// returns an error we ignore.
	for _, sm := range samples {
		_ = sm.ar.file.Sync()
		m.sampleChild(sm.ar, sm.pid)
	}
}

// sampleChild accounts CPU and memory of the current ffmpeg child into
// per-id metrics. CPU is exported as a counter that keeps growing across
// restarts. Only the persister goroutine touches the stat fields.
func (m *Manager) sampleChild(ar *activeRecording, pid int) {
	id := ar.s.ID // immutable
	if pid <= 0 {
		m.met.FFmpegRSS.WithLabelValues(id).Set(0)
		ar.statPID = 0
		return
	}
	st, err := sysmon.ProcStats(pid)
	if err != nil {
		return
	}
	if pid != ar.statPID {
		// New process: its counter starts from zero.
		ar.statPID = pid
		ar.statCPU = 0
	}
	if st.CPUSeconds > ar.statCPU {
		m.met.FFmpegCPUSeconds.WithLabelValues(id).Add(st.CPUSeconds - ar.statCPU)
		ar.statCPU = st.CPUSeconds
	}
	m.met.FFmpegRSS.WithLabelValues(id).Set(float64(st.RSS))
}

// RunRetention forgets terminal sessions older than the retention period.
func (m *Manager) RunRetention(ctx context.Context) {
	m.retention()
	t := time.NewTicker(10 * time.Minute)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			m.retention()
		}
	}
}

func (m *Manager) retention() {
	cutoff := time.Now().Add(-m.cfg.RetentionUploaded)
	m.mu.Lock()
	defer m.mu.Unlock()
	for sid, s := range m.sessions {
		if !s.IsTerminal() || s.UpdatedAt.After(cutoff) {
			continue
		}
		switch s.State {
		case StateUploaded:
			if _, err := os.Stat(s.FilePath()); err == nil {
				// The audio file still exists even though it was uploaded — never
				// throw metadata away while data is on disk.
				continue
			}
			_ = os.Remove(s.SidecarPath())
		case StateFailed:
			if st, err := os.Stat(s.FilePath()); err == nil && st.Size() == 0 {
				_ = os.Remove(s.FilePath())
			}
			if _, err := os.Stat(s.FilePath()); errors.Is(err, os.ErrNotExist) {
				_ = os.Remove(s.SidecarPath())
			} else {
				continue // keep metadata next to a non-empty failed file
			}
		case StateKept:
			// files stay on disk; only drop from memory
		}
		delete(m.sessions, sid)
		_ = os.Remove(s.dir) // succeeds only when empty
		stillKnown := false
		for _, other := range m.sessions {
			if other.ID == s.ID {
				stillKnown = true
				break
			}
		}
		if !stillKnown {
			m.met.ForgetRecording(s.ID)
		}
	}
}

// Shutdown stops every ffmpeg process concurrently (sessions stay resumable)
// and waits for in-flight uploads until ctx expires.
func (m *Manager) Shutdown(ctx context.Context) {
	m.mu.Lock()
	m.shuttingDown.Store(true)
	actives := make([]*activeRecording, 0, len(m.active))
	for _, ar := range m.active {
		if !ar.stopping {
			ar.stopping = true
			actives = append(actives, ar)
		}
	}
	m.mu.Unlock()
	m.log.Info("stopping recordings", "count", len(actives))

	var wg sync.WaitGroup
	for _, ar := range actives {
		wg.Add(1)
		go func(ar *activeRecording) {
			defer wg.Done()
			ar.cancel()
			if ar.supervised {
				<-ar.done
			}
			m.finalize(ar, ReasonShutdown)
		}(ar)
	}
	wg.Wait()
	m.stopWG.Wait()

	done := make(chan struct{})
	go func() {
		m.uploadWG.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-ctx.Done():
		m.log.Warn("cancelling in-flight uploads (they resume on next start)")
		m.uploadCancel()
		<-done
	}
	m.uploadCancel()
}

// ---------------------------------------------------------------------------
// Views for the HTTP API
// ---------------------------------------------------------------------------

// RecordingView is the API representation of a session.
type RecordingView struct {
	ID              string     `json:"id"`
	SessionID       string     `json:"sessionId"`
	Name            string     `json:"name,omitempty"`
	State           State      `json:"state"`
	FinishReason    string     `json:"finishReason,omitempty"`
	Source          string     `json:"source"`
	Type            string     `json:"type,omitempty"`
	Codec           string     `json:"codec"`
	ResolvedCodec   string     `json:"resolvedCodec,omitempty"`
	Start           time.Time  `json:"start"`
	End             time.Time  `json:"end"`
	SessionStart    time.Time  `json:"sessionStart"`
	SessionEnd      *time.Time `json:"sessionEnd,omitempty"`
	Bytes           int64      `json:"bytes"`
	Restarts        int        `json:"restarts"`
	Stalls          int        `json:"stalls"`
	WriteErrors     int        `json:"writeErrors,omitempty"`
	FFmpegRunning   bool       `json:"ffmpegRunning"`
	PID             int        `json:"pid,omitempty"`
	LastDataAt      *time.Time `json:"lastDataAt,omitempty"`
	LastError       string     `json:"lastError,omitempty"`
	ScheduleNote    string     `json:"scheduleNote,omitempty"`
	LastStderr      []string   `json:"lastStderr,omitempty"`
	Key             string     `json:"key"`
	Playlist        string     `json:"playlist,omitempty"`
	DurationSeconds float64    `json:"durationSeconds,omitempty"`
	UploadAttempts  int        `json:"uploadAttempts"`
	UploadBlocked   bool       `json:"uploadBlocked,omitempty"`
	UploadedAt      *time.Time `json:"uploadedAt,omitempty"`
	NextUploadAt    *time.Time `json:"nextUploadAt,omitempty"`
	File            string     `json:"file,omitempty"`
}

// UploadStats summarises upload queue state.
type UploadStats struct {
	Pending    int `json:"pending"`
	InProgress int `json:"inProgress"`
	Blocked    int `json:"blocked"`
	Uploaded   int `json:"uploaded"`
	Kept       int `json:"kept"`
	Failed     int `json:"failed"`
}

// Recordings returns every known session, active ones first.
func (m *Manager) Recordings() []RecordingView {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]RecordingView, 0, len(m.sessions))
	for _, s := range m.sessions {
		v := RecordingView{
			ID: s.ID, SessionID: s.SessionID, Name: s.Name, State: s.State, FinishReason: s.FinishReason,
			Source: RedactURL(s.Source), Type: s.Type, Codec: s.Codec, ResolvedCodec: s.ResolvedCodec,
			Start: s.Start, End: s.End, SessionStart: s.SessionStart, SessionEnd: s.SessionEnd,
			Bytes: s.Bytes, Restarts: s.Restarts, Stalls: s.Stalls, WriteErrors: s.WriteErrors,
			LastError: redactLine(s.LastError), ScheduleNote: s.ScheduleNote, Key: s.Key,
			DurationSeconds: s.DurationSeconds,
			UploadAttempts:  s.UploadAttempts, UploadBlocked: s.UploadBlocked, UploadedAt: s.UploadedAt,
			NextUploadAt: s.NextUploadAt, File: s.FilePath(),
		}
		if s.Key != "" {
			v.Playlist = playlistKey(s.Key)
		}
		if s.State == StateUploaded {
			v.File = ""
		}
		if ar, ok := m.active[s.ID]; ok && ar.s == s {
			v.Bytes = ar.bytes.Load()
			v.FFmpegRunning = ar.running.Load()
			v.PID = int(ar.pid.Load())
			if t := ar.lastDataTime(); !t.IsZero() {
				v.LastDataAt = &t
			}
			v.LastStderr = ar.stderr.Tail()
		}
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool {
		ai, aj := out[i].State == StateRecording, out[j].State == StateRecording
		if ai != aj {
			return ai
		}
		if ai {
			return out[i].Start.Before(out[j].Start)
		}
		return out[i].SessionStart.After(out[j].SessionStart)
	})
	return out
}

// State returns the recorder part of /api/state.
func (m *Manager) State() any {
	recs := m.Recordings()
	m.mu.Lock()
	info := m.info
	active := len(m.active)
	diskLow := m.diskLow.Load()
	var st UploadStats
	for _, s := range m.sessions {
		switch s.State {
		case StateFinalized:
			st.Pending++
			if s.UploadBlocked {
				st.Blocked++
			}
		case StateUploading:
			st.InProgress++
		case StateUploaded:
			st.Uploaded++
		case StateKept:
			st.Kept++
		case StateFailed:
			st.Failed++
		}
	}
	m.mu.Unlock()
	info.URL = RedactURL(info.URL)
	info.LastError = redactLine(info.LastError)
	return map[string]any{
		"schedule": info,
		"recordings": map[string]any{
			"active":         active,
			"ffmpeg":         m.FFmpegProcessCount(),
			"diskLow":        diskLow,
			"uploadsEnabled": m.up != nil,
			"items":          recs,
			"uploads":        st,
		},
	}
}

// ScheduleView returns fetch state plus the normalized items (credentials
// redacted: header values are never exposed).
func (m *Manager) ScheduleView() any {
	m.mu.Lock()
	defer m.mu.Unlock()
	items := []schedule.Item{}
	if m.sched != nil {
		items = make([]schedule.Item, 0, len(m.sched.Items))
		for _, it := range m.sched.Items {
			it.Source = RedactURL(it.Source)
			if len(it.Headers) > 0 {
				h := make(map[string]string, len(it.Headers))
				for k := range it.Headers {
					h[k] = "***"
				}
				it.Headers = h
			}
			items = append(items, it)
		}
	}
	info := m.info
	info.URL = RedactURL(info.URL)
	info.LastError = redactLine(info.LastError)
	return map[string]any{
		"info":  info,
		"items": items,
	}
}
