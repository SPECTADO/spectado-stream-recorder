package recorder

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
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
	// StateKept: capture finished and uploads are disabled; file kept locally.
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
// coalesced so a pathologically flapping recording cannot grow the sidecar (and
// the published playlist) without limit.
const maxRuns = 2000

// Run is one ffmpeg run that produced audio: the byte range it wrote and the
// wall-clock time of its first sample (the clock anchor of that range).
type Run struct {
	StartedAt       time.Time  `json:"startedAt"`
	EndedAt         *time.Time `json:"endedAt,omitempty"`
	Anchor          time.Time  `json:"anchor"`       // wall-clock time of the first sample
	AnchorSource    string     `json:"anchorSource"` // "hls-pdt" | "wallclock"
	Offset          int64      `json:"offset"`       // first byte of the run in the file
	Bytes           int64      `json:"bytes"`        // bytes written (frames + ID3 tags)
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
	// upload (completeUpload clears LastError, not this) so a finished object
	// still shows why it flapped.
	RecordLastError string     `json:"recordLastError,omitempty"`
	Exits           []RunExit  `json:"exits,omitempty"`        // last runs' exits (bounded)
	Runs            []Run      `json:"runs,omitempty"`         // per ffmpeg run byte ranges + clock anchors
	ScheduleNote    string     `json:"scheduleNote,omitempty"` // e.g. "item invalid in schedule since ..."
	SuspendedAt     *time.Time `json:"suspendedAt,omitempty"`  // set when paused for shutdown

	UploadAttempts  int        `json:"uploadAttempts"`
	KeyRenames      int        `json:"keyRenames,omitempty"`      // times the key was changed after a conflict
	UploadBlocked   bool       `json:"uploadBlocked,omitempty"`   // permanent-looking error; slow retries
	MediaUploaded   bool       `json:"mediaUploaded,omitempty"`   // audio object stored and verified; playlist may still be pending
	DurationSeconds float64    `json:"durationSeconds,omitempty"` // playback time derived from the ADTS frames
	UploadedAt      *time.Time `json:"uploadedAt,omitempty"`
	UploadETag      string     `json:"uploadEtag,omitempty"`
	NextUploadAt    *time.Time `json:"nextUploadAt,omitempty"`

	UpdatedAt time.Time `json:"updatedAt"`

	dir           string // directory holding the files (not serialized)
	runsCoalesced bool   // whether a coalesce has already been logged this session
}

// FilePath returns the path of the audio file.
func (s *Session) FilePath() string { return filepath.Join(s.dir, s.SessionID+".aac") }

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

// defaultFolder is the bucket folder of a session without an explicit key.
// Every folder holds the media files of one recording plus its index.m3u8.
//
//	{prefix}{scheduled start date, UTC}/{safeId}/
func defaultFolder(prefix string, s *Session) string {
	return prefix + s.Start.UTC().Format("2006-01-02") + "/" + s.SafeID + "/"
}

// mediaKey is the object key of the session's audio file inside folder. It
// reuses the local file name ({safeId}_{session start, UTC}.aac), which is
// unique per session and sorts chronologically within the folder.
func mediaKey(folder string, s *Session) string {
	return folder + s.SessionID + ".aac"
}

// playlistKey returns the key of the index.m3u8 listing the media object key
// (always the same folder).
func playlistKey(mediaKey string) string {
	dir := path.Dir(mediaKey)
	if dir == "." || dir == "/" {
		return "index.m3u8"
	}
	return dir + "/index.m3u8"
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

// withSuffix inserts suffix before the file extension: ("a/b.aac", "_x") ->
// "a/b_x.aac". Keys without an extension simply get the suffix appended.
func withSuffix(key, suffix string) string {
	ext := filepath.Ext(key)
	if strings.Contains(ext, "/") { // dot belongs to a directory component
		ext = ""
	}
	return strings.TrimSuffix(key, ext) + suffix + ext
}
