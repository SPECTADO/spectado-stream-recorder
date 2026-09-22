package recorder

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spectado/stream-recorder/internal/schedule"
)

// State of a recording session.
type State string

const (
	// StateRecording: ffmpeg is (or should be) capturing into the file.
	StateRecording State = "recording"
	// StateFinalized: capture finished, file closed, waiting for upload.
	StateFinalized State = "finalized"
	// StateUploading: an upload attempt is in progress.
	StateUploading State = "uploading"
	// StateUploaded: object stored, local file deleted.
	StateUploaded State = "uploaded"
	// StateKept: capture finished and uploads are disabled; the .m4a is kept
	// locally (the .aac stays instead when its remux failed).
	StateKept State = "kept"
	// StateFailed: terminal failure (e.g. empty recording).
	StateFailed State = "failed"
)

// Finish reasons.
const (
	ReasonEnded      = "ended"      // scheduled end (+stop-late) passed
	ReasonRemoved    = "removed"    // item disappeared from the schedule
	ReasonRotated    = "rotated"    // MAX_SESSION_DURATION reached; a new session follows
	ReasonSuperseded = "superseded" // an older unfinished session found next to a newer one
	ReasonShutdown   = "shutdown"   // process stopping; session resumes on next start
	ReasonError      = "error"
)

const sidecarSchemaVersion = 1

// maxExits bounds Session.Exits so a long, flapping recording cannot grow the
// sidecar without limit; only the most recent runs matter for diagnosis.
const maxExits = 20

// maxRuns bounds Session.Runs. When exceeded the two oldest adjacent runs are
// coalesced so a pathologically flapping recording cannot grow the sidecar
// without limit (the run table also lands in the .m4a description metadata,
// capped separately at 500 entries; see internal/recorder/objectmeta.go).
const maxRuns = 2000

// Run is one ffmpeg run that produced audio: the byte range it wrote and the
// wall-clock time of its first sample (the clock anchor of that range).
type Run struct {
	StartedAt       time.Time  `json:"startedAt"`
	EndedAt         *time.Time `json:"endedAt,omitempty"`
	Anchor          time.Time  `json:"anchor"`       // wall-clock time of the first sample
	AnchorSource    string     `json:"anchorSource"` // "hls-pdt" | "wallclock"
	Offset          int64      `json:"offset"`       // first byte of the run in the file
	Bytes           int64      `json:"bytes"`        // bytes written (frames + junk)
	Frames          int        `json:"frames"`
	DurationSeconds float64    `json:"durationSeconds"`
}

// RunExit records why one ffmpeg run ended (kept for diagnosis after upload).
type RunExit struct {
	At         time.Time `json:"at"`
	RanSeconds float64   `json:"ranSeconds"`
	Bytes      int64     `json:"bytes"`
	Reason     string    `json:"reason"`
	ExitError  string    `json:"exitError,omitempty"`  // runErr.Error() for non-zero exits / write errors
	Stderr     string    `json:"stderr,omitempty"`     // last meaningful stderr line (post filter)
	ErrorLine  string    `json:"errorLine,omitempty"`  // last error-level line of the run, if any
	Suppressed int       `json:"suppressed,omitempty"` // benign lines dropped during the run
}

// Session is the persistent description of one recording session. It is
// stored next to the audio file as <session>.json so the recorder can resume
// or upload after a restart.
type Session struct {
	SchemaVersion int    `json:"schemaVersion"`
	ID            string `json:"id"`
	SafeID        string `json:"safeId"`
	SessionID     string `json:"sessionId"`

	Name         string             `json:"name,omitempty"`
	Source       string             `json:"source"`
	Type         string             `json:"type,omitempty"`
	Start        time.Time          `json:"start"`                  // scheduled start (latest known)
	End          time.Time          `json:"end"`                    // scheduled end (latest known)
	StartEarly   *schedule.Duration `json:"startEarly,omitempty"`   // per-item override
	StopLate     *schedule.Duration `json:"stopLate,omitempty"`     // per-item override
	StallTimeout *schedule.Duration `json:"stallTimeout,omitempty"` // per-item override
	InsecureTLS  bool               `json:"insecureTLS,omitempty"`
	Key          string             `json:"key"`                   // final object key
	KeyTemplate  string             `json:"keyTemplate,omitempty"` // explicit key from the schedule item, if any
	Codec        string             `json:"codec"`                 // requested: auto|aac|copy
	Bitrate      string             `json:"bitrate,omitempty"`
	Headers      map[string]string  `json:"headers,omitempty"`

	ResolvedCodec string     `json:"resolvedCodec,omitempty"` // aac|copy after probing
	SessionStart  time.Time  `json:"sessionStart"`
	SessionEnd    *time.Time `json:"sessionEnd,omitempty"`
	State         State      `json:"state"`
	Bytes         int64      `json:"bytes"`
	Size          int64      `json:"size,omitempty"` // file size at finalisation
	TrimmedBytes  int64      `json:"trimmedBytes,omitempty"`
	Restarts      int        `json:"restarts"`
	Stalls        int        `json:"stalls"`
	WriteErrors   int        `json:"writeErrors,omitempty"`
	FinishReason  string     `json:"finishReason,omitempty"`
	LastError     string     `json:"lastError,omitempty"`
	// RecordLastError is why the *recording* last restarted, built from the
	// filtered stderr. Set only by supervise; unlike LastError it survives the
	// upload (completeGroup clears LastError, not this) so a finished object
	// still shows why it flapped.
	RecordLastError string     `json:"recordLastError,omitempty"`
	Exits           []RunExit  `json:"exits,omitempty"`        // last runs' exits (bounded)
	Runs            []Run      `json:"runs,omitempty"`         // per ffmpeg run byte ranges + clock anchors
	ScheduleNote    string     `json:"scheduleNote,omitempty"` // e.g. "item invalid in schedule since ..."
	SuspendedAt     *time.Time `json:"suspendedAt,omitempty"`  // set when paused for shutdown

	UploadAttempts  int        `json:"uploadAttempts"`
	KeyRenames      int        `json:"keyRenames,omitempty"`      // times the key was changed after a conflict
	UploadBlocked   bool       `json:"uploadBlocked,omitempty"`   // permanent-looking error; slow retries
	MediaUploaded   bool       `json:"mediaUploaded,omitempty"`   // this session's audio is part of the object under Key
	DurationSeconds float64    `json:"durationSeconds,omitempty"` // playback time of THIS session derived from its ADTS frames
	UploadedAt      *time.Time `json:"uploadedAt,omitempty"`
	UploadETag      string     `json:"uploadEtag,omitempty"`
	NextUploadAt    *time.Time `json:"nextUploadAt,omitempty"`
	// Transcoded records that the object had to be re-encoded because the
	// stream parameters changed mid-capture (a stream copy would have been
	// short/pitched). RemuxFailures is the key's consecutive remux-failure
	// count, mirrored here for the API.
	Transcoded    bool   `json:"transcoded,omitempty"`
	RemuxFailures int    `json:"remuxFailures,omitempty"`
	OutputFile    string `json:"outputFile,omitempty"` // kept mode: the local .m4a produced from the capture

	UpdatedAt time.Time `json:"updatedAt"`

	dir           string // directory holding the files (not serialized)
	runsCoalesced bool   // whether a coalesce has already been logged this session
}

// FilePath returns the path of the raw ADTS capture. Recording stays raw ADTS
// on disk (crash-safe append, resume, tail trimming); the .m4a is produced from
// it at upload time.
func (s *Session) FilePath() string { return filepath.Join(s.dir, s.SessionID+".aac") }

// OutputPath is where kept mode (UPLOAD_DISABLED=true) writes the remuxed file.
func (s *Session) OutputPath() string { return filepath.Join(s.dir, s.SessionID+".m4a") }

// setKey is the ONLY way Session.Key may change. Moving a session to another
// key invalidates everything we know about the object it used to belong to: an
// object under a different key never contains this session, so a stale
// MediaUploaded would make the upload skip audio that was never stored there.
func (s *Session) setKey(k string) {
	if s.Key == k {
		return
	}
	s.Key = k
	s.MediaUploaded = false
	s.UploadETag = ""
}

// SidecarPath returns the path of the metadata file.
func (s *Session) SidecarPath() string { return filepath.Join(s.dir, s.SessionID+".json") }

// Dir returns the session directory.
func (s *Session) Dir() string { return s.dir }

// IsTerminal reports whether the session needs no more work.
func (s *Session) IsTerminal() bool {
	return s.State == StateUploaded || s.State == StateKept || s.State == StateFailed
}

// effectiveStopLate returns the per-item override or the default.
func (s *Session) effectiveStopLate(def time.Duration) time.Duration {
	if s.StopLate != nil {
		return s.StopLate.D()
	}
	return def
}

// applyItem copies mutable schedule fields into the session.
func (s *Session) applyItem(it schedule.Item) (sourceChanged bool) {
	sourceChanged = s.Source != "" && it.Source != s.Source
	s.Name = it.Name
	s.Source = it.Source
	s.Type = it.Type
	s.Start = it.Start
	s.End = it.End
	s.Headers = it.Headers
	s.StartEarly = it.StartEarly
	s.StopLate = it.StopLate
	s.StallTimeout = it.StallTimeout
	s.InsecureTLS = it.InsecureTLS
	if it.Codec != "" {
		s.Codec = it.Codec
	}
	if it.Bitrate != "" {
		s.Bitrate = it.Bitrate
	}
	if sourceChanged {
		s.ResolvedCodec = "" // re-probe against the new source
	}
	return sourceChanged
}

// save writes the sidecar atomically with fsync (state transitions).
func (s *Session) save() error {
	data, err := s.encode()
	if err != nil {
		return err
	}
	return schedule.WriteFileAtomic(s.SidecarPath(), data, 0o644)
}

// saveQuick writes the sidecar atomically without fsync (frequent progress
// updates; durability is left to the OS).
func (s *Session) saveQuick() error {
	data, err := s.encode()
	if err != nil {
		return err
	}
	return schedule.WriteFileAtomicNoSync(s.SidecarPath(), data, 0o644)
}

func (s *Session) encode() ([]byte, error) {
	s.SchemaVersion = sidecarSchemaVersion
	s.UpdatedAt = time.Now()
	return json.MarshalIndent(s, "", "  ")
}

// loadSession reads a sidecar file.
func loadSession(path string) (*Session, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var s Session
	if err := json.Unmarshal(data, &s); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if s.ID == "" || s.SessionID == "" {
		return nil, fmt.Errorf("parse %s: missing id/sessionId", path)
	}
	s.dir = filepath.Dir(path)
	if s.SafeID == "" {
		s.SafeID = schedule.SafeID(s.ID)
	}
	if s.Codec == "" {
		s.Codec = "auto"
	}
	return &s, nil
}

// scanSessions loads every sidecar under root/<safeId>/*.json and removes
// leftover temporary files.
func scanSessions(root string) ([]*Session, []error) {
	var out []*Session
	var errs []error
	entries, err := os.ReadDir(root)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, []error{err}
	}
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(root, e.Name())
		files, err := os.ReadDir(dir)
		if err != nil {
			errs = append(errs, err)
			continue
		}
		for _, f := range files {
			name := f.Name()
			if f.IsDir() {
				continue
			}
			if strings.HasPrefix(name, ".") && strings.Contains(name, ".tmp-") {
				_ = os.Remove(filepath.Join(dir, name))
				continue
			}
			if !strings.HasSuffix(name, ".json") || strings.HasPrefix(name, ".") {
				continue
			}
			s, err := loadSession(filepath.Join(dir, name))
			if err != nil {
				errs = append(errs, err)
				continue
			}
			out = append(out, s)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].SessionStart.Before(out[j].SessionStart) })
	return out, errs
}

// objectExt is the suffix of every recorded object: one .m4a per recording.
const objectExt = ".m4a"

// objectKey is the key of the single object holding a recording that has no
// explicit schedule key:
//
//	{prefix}{scheduled start date in loc}/{safeId}.m4a
//
// e.g. "2026-09-22/match-ro-jpOkle8Mp0.m4a". loc is KEY_DATE_TZ (UTC by
// default): a show starting at 23:30 UTC belongs to the next day for a
// broadcaster in Prague, and the folder has to follow the broadcaster.
func objectKey(prefix string, loc *time.Location, s *Session) string {
	if loc == nil {
		loc = time.UTC
	}
	return prefix + s.Start.In(loc).Format("2006-01-02") + "/" + s.SafeID + objectExt
}

// explicitObjectKey is the key of a recording whose schedule item carries a
// key: since 1.1.0 that key names the OBJECT, not a folder. ".m4a" is appended
// unless the key already ends in it (case-insensitively, because a producer
// writing ".M4A" means the same object).
func explicitObjectKey(prefix, key string) string {
	k := prefix + strings.Trim(strings.TrimSpace(key), "/")
	if strings.HasSuffix(strings.ToLower(k), objectExt) {
		return k
	}
	return k + objectExt
}

// sessionDigest identifies a session inside an object's `sessions` manifest:
// the first 8 hex characters of sha256(sessionId). The session ids themselves
// would blow the metadata budget (20 parts x ~40 B), while 8 hex characters are
// 4 bytes of collision resistance over the at most 20 sessions of one object.
func sessionDigest(sessionID string) string {
	sum := sha256.Sum256([]byte(sessionID))
	return hex.EncodeToString(sum[:4])
}

// addExit appends e to the session's exit history, keeping only the most recent
// maxExits entries (oldest dropped). Caller holds Manager.mu.
func (s *Session) addExit(e RunExit) {
	s.Exits = append(s.Exits, e)
	if len(s.Exits) > maxExits {
		s.Exits = s.Exits[len(s.Exits)-maxExits:]
	}
}

// appendRun appends r to the session's run history. When the cap is exceeded the
// two OLDEST adjacent runs are coalesced into one (their bytes/frames/duration
// summed, the first anchor/start kept, the later end taken). It returns true the
// first time a coalesce happens so the caller can log it once. Caller holds
// Manager.mu.
func (s *Session) appendRun(r Run) (coalescedNow bool) {
	s.Runs = append(s.Runs, r)
	if len(s.Runs) <= maxRuns {
		return false
	}
	a, b := s.Runs[0], s.Runs[1]
	a.Bytes += b.Bytes
	a.Frames += b.Frames
	a.DurationSeconds += b.DurationSeconds
	a.EndedAt = laterTime(a.EndedAt, b.EndedAt)
	merged := make([]Run, 0, len(s.Runs)-1)
	merged = append(merged, a)
	merged = append(merged, s.Runs[2:]...)
	s.Runs = merged
	if !s.runsCoalesced {
		s.runsCoalesced = true
		return true
	}
	return false
}

// openRun returns a pointer to the last, still-open run (EndedAt == nil), or nil
// when there is none. Caller holds Manager.mu.
func (s *Session) openRun() *Run {
	if n := len(s.Runs); n > 0 && s.Runs[n-1].EndedAt == nil {
		return &s.Runs[n-1]
	}
	return nil
}

// fixOpenRun closes an open run left by a crash (no clean endRun): its end is
// the suspend time if known else now, and its byte length is the (already
// trimmed) file size minus its offset. Frames/duration are recomputed by
// ScanRuns at finalize. Caller holds Manager.mu.
func (s *Session) fixOpenRun(size int64, now time.Time) {
	r := s.openRun()
	if r == nil {
		return
	}
	end := now
	if s.SuspendedAt != nil {
		end = *s.SuspendedAt
	}
	r.EndedAt = &end
	if size >= r.Offset {
		r.Bytes = size - r.Offset
	}
}

// laterTime returns the later of two optional times.
func laterTime(a, b *time.Time) *time.Time {
	switch {
	case a == nil:
		return b
	case b == nil:
		return a
	case b.After(*a):
		return b
	default:
		return a
	}
}

// exitReasonsSummary renders a compact, deterministic "reason=count" list of
// the session's exits, sorted by reason (e.g. "demux-error=3,stream-ended=1").
// Empty when there were no exits. Used for small S3 object metadata.
func exitReasonsSummary(exits []RunExit) string {
	if len(exits) == 0 {
		return ""
	}
	counts := make(map[string]int, len(exits))
	for _, e := range exits {
		counts[e.Reason]++
	}
	reasons := make([]string, 0, len(counts))
	for r := range counts {
		reasons = append(reasons, r)
	}
	sort.Strings(reasons)
	parts := make([]string, 0, len(reasons))
	for _, r := range reasons {
		parts = append(parts, r+"="+strconv.Itoa(counts[r]))
	}
	return strings.Join(parts, ",")
}

// withSuffix inserts suffix before the file extension: ("a/b.m4a", "-2") ->
// "a/b-2.m4a". Keys without an extension simply get the suffix appended.
func withSuffix(key, suffix string) string {
	ext := filepath.Ext(key)
	if strings.Contains(ext, "/") { // dot belongs to a directory component
		ext = ""
	}
	return strings.TrimSuffix(key, ext) + suffix + ext
}
