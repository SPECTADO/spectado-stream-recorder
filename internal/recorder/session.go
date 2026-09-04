package recorder

import (
	"encoding/json"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
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
	ScheduleNote  string     `json:"scheduleNote,omitempty"` // e.g. "item invalid in schedule since ..."
	SuspendedAt   *time.Time `json:"suspendedAt,omitempty"`  // set when paused for shutdown

	UploadAttempts  int        `json:"uploadAttempts"`
	KeyRenames      int        `json:"keyRenames,omitempty"`      // times the key was changed after a conflict
	UploadBlocked   bool       `json:"uploadBlocked,omitempty"`   // permanent-looking error; slow retries
	MediaUploaded   bool       `json:"mediaUploaded,omitempty"`   // audio object stored and verified; playlist may still be pending
	DurationSeconds float64    `json:"durationSeconds,omitempty"` // playback time derived from the ADTS frames
	UploadedAt      *time.Time `json:"uploadedAt,omitempty"`
	UploadETag      string     `json:"uploadEtag,omitempty"`
	NextUploadAt    *time.Time `json:"nextUploadAt,omitempty"`

	UpdatedAt time.Time `json:"updatedAt"`

	dir string // directory holding the files (not serialized)
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

// withSuffix inserts suffix before the file extension: ("a/b.aac", "_x") ->
// "a/b_x.aac". Keys without an extension simply get the suffix appended.
func withSuffix(key, suffix string) string {
	ext := filepath.Ext(key)
	if strings.Contains(ext, "/") { // dot belongs to a directory component
		ext = ""
	}
	return strings.TrimSuffix(key, ext) + suffix + ext
}
