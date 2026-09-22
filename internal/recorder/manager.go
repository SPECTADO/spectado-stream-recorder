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
	"github.com/spectado/stream-recorder/internal/remux"
	"github.com/spectado/stream-recorder/internal/schedule"
	"github.com/spectado/stream-recorder/internal/sysmon"
)

// Uploader stores finished recordings — one object per recording — and gives
// the recorder back what it needs to extend an object it wrote earlier.
// Implementations must only return nil from Upload once the object is durably
// stored and verified.
type Uploader interface {
	// Upload stores the file at path under key. With an empty replaceETag it
	// must not overwrite a different existing object (see ErrObjectExists);
	// with one it must replace exactly the object that has that ETag and
	// return ErrObjectChanged otherwise.
	Upload(ctx context.Context, path, key, contentType string, metadata map[string]string, replaceETag string) (etag string, err error)
	// Head reports size, ETag and user metadata of key; found is false when
	// there is no such object.
	Head(ctx context.Context, key string) (info ObjectInfo, found bool, err error)
	// Download writes the object at key to path, failing with ErrObjectChanged
	// when ifMatchETag is given and no longer matches.
	Download(ctx context.Context, key, ifMatchETag, path string) error
	// ConditionalWrites reports whether the endpoint enforces
	// If-None-Match/If-Match on single-part AND multipart uploads (probed once
	// with a tiny object; cached). Merging several sessions into one object is
	// only safe when it does.
	ConditionalWrites(ctx context.Context) (bool, error)
}

// Remuxer turns raw ADTS captures into the .m4a that is stored, and back. It is
// the subset of *remux.Remuxer the manager uses, as an interface so the upload
// path can be tested without ffmpeg.
type Remuxer interface {
	Build(ctx context.Context, parts []string, out string, expect remux.Expect, meta remux.Metadata) (remux.Result, error)
	BuildTranscode(ctx context.Context, chunks []remux.Chunk, out string, expectedDuration time.Duration, bitrate string, meta remux.Metadata) (remux.Result, error)
	Extract(ctx context.Context, in, out string) error
	Probe(ctx context.Context, path string) (remux.Result, error)
}

// ErrObjectExists is returned by an Uploader when the key is already taken by
// a different object (conditional put failed). The manager picks a new key.
var ErrObjectExists = errors.New("object already exists with different content")

// ErrObjectChanged is returned by an Uploader when a replacing upload
// (If-Match on the ETag read earlier) finds the object was modified in between.
// The manager re-reads the object and merges again.
var ErrObjectChanged = errors.New("object changed since it was read")

// ObjectInfo describes a stored object as reported by a HEAD request.
type ObjectInfo struct {
	Size     int64
	ETag     string            // without surrounding quotes
	Metadata map[string]string // user metadata (x-amz-meta-*), keys lower-case
}

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
	rx            Remuxer
	sched         *schedule.Schedule
	info          ScheduleInfo
	active        map[string]*activeRecording // item id -> capture in progress
	sessions      map[string]*Session         // session id -> every known session
	uploadLoops   map[string]bool             // session id -> upload goroutine alive
	skipLogged    map[string]time.Time
	loggedInvalid map[string]struct{}
	remuxFailures map[string]int       // object key -> consecutive remux failures
	nospaceLogged map[string]time.Time // object key -> last "no disk" warning
	warnedSafeID  map[string]struct{}  // recording ids whose object name differs
	warnedKeys    map[string]struct{}  // explicit schedule keys already explained
	mergeProbed   bool                 // ConditionalWrites has been asked
	mergeAllowed  bool                 // ... and said yes
	shuttingDown  atomic.Bool
	schedLoaded   atomic.Bool
	activeCount   atomic.Int64
	diskLow       atomic.Bool
	diskFree      func() (free uint64, ok bool)

	uploadCtx    context.Context
	uploadCancel context.CancelFunc
	uploadWG     sync.WaitGroup
	stopWG       sync.WaitGroup
	keyLocks     keyedLocks // serialises everything that touches one object key
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
		remuxFailures: map[string]int{},
		nospaceLogged: map[string]time.Time{},
		warnedSafeID:  map[string]struct{}{},
		warnedKeys:    map[string]struct{}{},
		diskFree:      func() (uint64, bool) { return 0, false },
		uploadCtx:     uctx,
		uploadCancel:  ucancel,
	}
	// The remuxer runs the same ffmpeg/ffprobe binaries as the capture; the
	// transfer throughput assumption doubles as the remux throughput floor.
	m.rx = &remux.Remuxer{
		FFmpegPath:    cfg.FFmpegPath,
		FFprobePath:   cfg.FFprobePath,
		MinThroughput: cfg.UploadMinThroughput,
		Log:           m.log.With("component", "remux"),
	}
	m.info.PollInterval = cfg.SchedulePollInterval.String()
	return m
}

// SetRemuxer replaces the ffmpeg-backed remuxer (tests inject a fake so the
// upload path can run without ffmpeg).
func (m *Manager) SetRemuxer(r Remuxer) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if r != nil {
		m.rx = r
	}
}

// remuxer returns the installed remuxer (read under the lock so SetRemuxer can
// be called while goroutines are already running).
func (m *Manager) remuxer() Remuxer {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.rx
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
	ar.stderr.onSuppressed = func(reason string) {
		m.met.FFmpegStderrSuppressedTotal.WithLabelValues(reason).Inc()
	}
	ar.bytes.Store(size)
	ar.savedBytes = size
	ar.runningGauge.Set(0)
	return ar, nil
}

// keyForLocked returns the object key a session must use.
//
// Key pinning: a session whose recording id is already known keeps that
// recording's object as long as the earlier session's scheduled end is less
// than keyPinWindow in the past — an extension after the end, a re-added item,
// a rotation or a start moved across midnight must all land in the SAME object,
// not in a second one named after a different day. The newest such session
// wins. Only keys that name an .m4a are inherited: a 1.0.x key and the raw-ADTS
// fallback key are dead ends nothing may be appended to.
func (m *Manager) keyForLocked(s *Session) (key string, inherited bool) {
	var best *Session
	for _, o := range m.sessions {
		if o == s || o.ID != s.ID || !strings.HasSuffix(strings.ToLower(o.Key), objectExt) {
			continue
		}
		if !o.End.Add(keyPinWindow).After(s.Start) {
			continue
		}
		if best == nil || o.SessionStart.After(best.SessionStart) {
			best = o
		}
	}
	if best != nil {
		// The rename counter travels with the key: the group must agree on how
		// often it has already been moved aside.
		s.KeyRenames = best.KeyRenames
		return best.Key, true
	}
	if s.KeyTemplate != "" {
		return explicitObjectKey(m.cfg.S3Prefix, s.KeyTemplate), false
	}
	return objectKey(m.cfg.S3Prefix, m.cfg.KeyDateLocation, s), false
}

// warnAboutKeyLocked says once per recording id (and once per explicit key)
// what the object will actually be called: since 1.1.0 the key IS the file
// someone downloads, so a sanitised id or a key that used to name a folder
// changes what the operator finds in the bucket.
func (m *Manager) warnAboutKeyLocked(s *Session) {
	if s.SafeID != s.ID {
		if _, done := m.warnedSafeID[s.ID]; !done {
			m.warnedSafeID[s.ID] = struct{}{}
			m.log.Warn("recording id is not usable as an object name; the object is named after a sanitised id",
				"id", s.ID, "safeId", s.SafeID, "key", s.Key)
		}
	}
	if s.KeyTemplate != "" {
		if _, done := m.warnedKeys[s.KeyTemplate]; !done {
			m.warnedKeys[s.KeyTemplate] = struct{}{}
			m.log.Warn("the schedule item's key now names a single object, not a folder",
				"id", s.ID, "itemKey", s.KeyTemplate, "key", s.Key)
		}
	}
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
	// One object per recording: every session of this show (rotation, extension
	// after the end, a re-added item) is merged into the same object, so the
	// key is pinned to whatever an earlier session of the same id already uses.
	if it.Key != "" {
		s.KeyTemplate = it.Key
	}
	key, inherited := m.keyForLocked(s)
	s.setKey(key)
	if inherited {
		m.log.Info("object key inherited from an earlier session of this recording", "id", it.ID, "session", sid, "key", key)
	}
	m.warnAboutKeyLocked(s)

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
	// Close a still-open run (the ctx-cancel path returns from supervise before
	// its own endRun). Idempotent, and must run before the file is closed because
	// it flushes the buffered partial frame.
	m.endRun(ar)
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
		m.migrateKeyLocked(s)
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
			// A crash may have left the last run open (no clean endRun): close it
			// using the trimmed file size before nil-ing SuspendedAt, so its end
			// time is the suspend time when known.
			s.fixOpenRun(ar.bytes.Load(), time.Now())
			s.SuspendedAt = nil
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
				// the object's session manifest tells the attempt that this
				// session is already stored, so nothing is uploaded twice.
				m.log.Warn("uploaded recording still has its local file; re-verifying", "id", s.ID, "session", s.SessionID)
				s.State = StateFinalized
				toUpload = append(toUpload, s)
			}
		case StateKept:
			// Kept mode: a capture that still has no .m4a never finished its
			// remux (crash, or the process stopped before the worker ran).
			if m.up != nil {
				continue
			}
			if _, err := os.Stat(s.FilePath()); err != nil {
				continue
			}
			if _, err := os.Stat(s.OutputPath()); errors.Is(err, os.ErrNotExist) {
				m.log.Info("kept recording has no remuxed file yet; remuxing again", "id", s.ID, "session", s.SessionID)
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

// migrateKeyLocked upgrades a sidecar written by 1.0.x, whose Key names a
// per-session .aac inside a folder, to the 1.1.0 layout where the key names the
// one object of the recording. Terminal sessions are left alone: their .aac
// object exists in the bucket and renaming them would only lose the record of
// where it is.
func (m *Manager) migrateKeyLocked(s *Session) {
	if s.IsTerminal() || strings.HasSuffix(strings.ToLower(s.Key), objectExt) {
		return
	}
	old := s.Key
	key, _ := m.keyForLocked(s)
	s.setKey(key)
	if err := s.save(); err != nil {
		m.log.Warn("save sidecar", "id", s.ID, "session", s.SessionID, "error", err)
	}
	m.log.Info("migrated recording to the 1.1.0 single-object layout", "id", s.ID, "session", s.SessionID,
		"oldKey", old, "newKey", s.Key)
}

// warnOrphans logs audio files that have no sidecar (nothing is deleted). A
// kept .m4a next to its .json is not an orphan, so both extensions count.
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
			name := f.Name()
			ext := filepath.Ext(name)
			if ext != ".aac" && ext != objectExt {
				continue
			}
			sidecar := filepath.Join(m.root, e.Name(), strings.TrimSuffix(name, ext)+".json")
			if _, err := os.Stat(sidecar); err != nil {
				m.log.Warn("orphan recording without metadata (left untouched)", "file", filepath.Join(m.root, e.Name(), name))
			}
		}
	}
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
		// Copy the live run counters into the open run so a crash loses at most
		// one persist interval of counters (~10 s).
		if r := ar.s.openRun(); r != nil {
			ar.run.mu.Lock()
			r.Bytes = ar.run.bytes
			r.Frames = ar.run.frames
			r.DurationSeconds = ar.run.seconds
			ar.run.mu.Unlock()
		}
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
	ID               string     `json:"id"`
	SessionID        string     `json:"sessionId"`
	Name             string     `json:"name,omitempty"`
	State            State      `json:"state"`
	FinishReason     string     `json:"finishReason,omitempty"`
	Source           string     `json:"source"`
	Type             string     `json:"type,omitempty"`
	Codec            string     `json:"codec"`
	ResolvedCodec    string     `json:"resolvedCodec,omitempty"`
	Start            time.Time  `json:"start"`
	End              time.Time  `json:"end"`
	SessionStart     time.Time  `json:"sessionStart"`
	SessionEnd       *time.Time `json:"sessionEnd,omitempty"`
	Bytes            int64      `json:"bytes"`
	Restarts         int        `json:"restarts"`
	Stalls           int        `json:"stalls"`
	WriteErrors      int        `json:"writeErrors,omitempty"`
	FFmpegRunning    bool       `json:"ffmpegRunning"`
	PID              int        `json:"pid,omitempty"`
	LastDataAt       *time.Time `json:"lastDataAt,omitempty"`
	LastError        string     `json:"lastError,omitempty"`
	RecordLastError  string     `json:"recordLastError,omitempty"`
	Exits            []RunExit  `json:"exits,omitempty"`
	Runs             []Run      `json:"runs,omitempty"`             // per-run byte ranges + clock anchors (all states)
	ClockAnchor      *time.Time `json:"clockAnchor,omitempty"`      // wall clock of the current run's first sample (active)
	ClockSource      string     `json:"clockSource,omitempty"`      // "hls-pdt" | "wallclock" (active)
	StderrSuppressed int        `json:"stderrSuppressed,omitempty"` // benign lines dropped this run (active only)
	ScheduleNote     string     `json:"scheduleNote,omitempty"`
	LastStderr       []string   `json:"lastStderr,omitempty"`
	Key              string     `json:"key"` // the single object this recording is stored as
	DurationSeconds  float64    `json:"durationSeconds,omitempty"`
	UploadAttempts   int        `json:"uploadAttempts"`
	UploadBlocked    bool       `json:"uploadBlocked,omitempty"`
	UploadedAt       *time.Time `json:"uploadedAt,omitempty"`
	NextUploadAt     *time.Time `json:"nextUploadAt,omitempty"`
	File             string     `json:"file,omitempty"`       // local capture, while it exists
	OutputFile       string     `json:"outputFile,omitempty"` // kept mode: the local .m4a
	RemuxFailures    int        `json:"remuxFailures,omitempty"`
	Transcoded       bool       `json:"transcoded,omitempty"` // the object had to be re-encoded
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
			LastError: redactLine(s.LastError), RecordLastError: redactLine(s.RecordLastError),
			ScheduleNote: s.ScheduleNote, Key: s.Key,
			DurationSeconds: s.DurationSeconds,
			UploadAttempts:  s.UploadAttempts, UploadBlocked: s.UploadBlocked, UploadedAt: s.UploadedAt,
			NextUploadAt: s.NextUploadAt, File: s.FilePath(), OutputFile: s.OutputFile,
			RemuxFailures: s.RemuxFailures, Transcoded: s.Transcoded,
		}
		// exits[] and runs[] are shown for ALL states so finished/uploaded items
		// still reveal why they restarted and where each run's clock sits; copy
		// the slices so the view never aliases the session.
		if len(s.Exits) > 0 {
			v.Exits = append([]RunExit(nil), s.Exits...)
		}
		if len(s.Runs) > 0 {
			v.Runs = append([]Run(nil), s.Runs...)
		}
		if s.State == StateUploaded {
			v.File = ""
		}
		if s.State == StateKept && s.OutputFile != "" {
			// The capture is gone once the remux succeeded; the .m4a is the file.
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
			v.StderrSuppressed = ar.stderr.suppressedRun()
			ar.run.mu.Lock()
			if ar.run.started {
				a := ar.run.anchor
				v.ClockAnchor = &a
				v.ClockSource = ar.run.anchorSource
			}
			ar.run.mu.Unlock()
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
