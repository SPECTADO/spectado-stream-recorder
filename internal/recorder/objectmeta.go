package recorder

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"mime"
	"strconv"
	"strings"
	"time"

	"github.com/spectado/stream-recorder/internal/remux"
)

// maxRunTableEntries bounds the JSON run table embedded in the .m4a. A
// recording that flapped for a day would otherwise carry thousands of entries
// into every later merge; the oldest are coalesced, never dropped, so the map
// stays complete (coarser at the front).
const maxRunTableEntries = 500

// milliLayout is how wall-clock instants are written in metadata: RFC 3339 in
// UTC with milliseconds, the precision the run anchors have.
const milliLayout = "2006-01-02T15:04:05.000Z07:00"

// runEntry is one ffmpeg run in the object's position -> wall-clock map. It is
// what replaced the playlist's #EXT-X-PROGRAM-DATE-TIME: `off` is where the
// run's first sample sits inside the object (media seconds) and `t` the wall
// clock of that sample. Restart overlaps and gaps are NOT trimmed from the
// audio, so a consumer that needs real time maps through this table.
type runEntry struct {
	SID string  `json:"sid"`
	T   string  `json:"t"`
	Src string  `json:"src"`
	Off float64 `json:"off"`
	Dur float64 `json:"dur"`
}

// objectSummary is everything the object's metadata says about the sessions it
// holds: the remote object's own manifest extended by the sessions this attempt
// adds. Collected once, under Manager.mu, and then used without the lock.
type objectSummary struct {
	recordingID  string
	name         string
	source       string // already redacted
	finishReason string
	clockSource  string
	digests      []string
	parts        int
	schedStart   time.Time
	schedEnd     time.Time
	sessStart    time.Time
	sessEnd      time.Time
	lastStart    time.Time
	firstSample  time.Time
	restarts     int
	runs         int
	exits        []RunExit
}

// summarize folds the stored object's manifest (when merging) and the sessions
// this attempt adds into one view. The remote values are the base — the object
// keeps the identity of its first part — and the new sessions extend them.
func (m *Manager) summarize(toAdd []*uploadMember, remote *remoteObject) objectSummary {
	m.mu.Lock()
	defer m.mu.Unlock()

	var sum objectSummary
	var remoteName string
	setAnchor := func(t time.Time, src string) {
		if t.IsZero() {
			return
		}
		if sum.firstSample.IsZero() || t.Before(sum.firstSample) {
			sum.firstSample, sum.clockSource = t, src
		}
	}
	if remote != nil {
		sum.recordingID = remote.recordingID
		remoteName = remote.meta["name"]
		sum.source = remote.meta["source"]
		sum.digests = append(sum.digests, remote.sessions...)
		sum.parts = remote.parts
		sum.schedStart = parseMetaTime(remote.meta["scheduled-start"])
		sum.schedEnd = parseMetaTime(remote.meta["scheduled-end"])
		sum.sessStart = parseMetaTime(remote.meta["session-start"])
		sum.sessEnd = parseMetaTime(remote.meta["session-end"])
		sum.lastStart = remote.lastStart
		sum.restarts = atoiMeta(remote.meta["ffmpeg-restarts"])
		sum.runs = atoiMeta(remote.meta["ffmpeg-runs"])
		setAnchor(parseMetaTime(remote.meta["first-sample-time"]), remote.meta["clock-source"])
	}
	for _, mem := range toAdd {
		s := mem.s
		if sum.recordingID == "" {
			sum.recordingID = s.ID
		}
		if sum.name == "" {
			sum.name = s.Name
		}
		sum.source = RedactURL(s.Source)
		sum.finishReason = s.FinishReason
		sum.digests = append(sum.digests, mem.digest)
		sum.parts++
		sum.schedStart = earliest(sum.schedStart, s.Start)
		sum.schedEnd = latest(sum.schedEnd, s.End)
		sum.sessStart = earliest(sum.sessStart, s.SessionStart)
		if s.SessionEnd != nil {
			sum.sessEnd = latest(sum.sessEnd, *s.SessionEnd)
		}
		sum.lastStart = latest(sum.lastStart, s.SessionStart)
		sum.restarts += s.Restarts
		sum.runs += len(s.Runs)
		sum.exits = append(sum.exits, s.Exits...)
		if len(s.Runs) > 0 {
			setAnchor(s.Runs[0].Anchor, s.Runs[0].AnchorSource)
		} else {
			setAnchor(s.SessionStart, "wallclock")
		}
	}
	// The name is the one value the remote object is a WORSE source for than the
	// local sidecar: storage RFC-2047-encodes it when it is not printable ASCII
	// and truncates it to fit the metadata budget, so reading it back and
	// writing it out again would turn "Rádio Jedna" into "=?utf-8?b?...?=" (and
	// the .m4a title with it). A local member always has the real name; the
	// remote value is only for an object whose sessions are all gone from disk.
	if sum.name == "" {
		sum.name = decodeMetaName(remoteName)
	}
	return sum
}

// decodeMetaName reads a name back out of object metadata, undoing the RFC 2047
// encoding storage applies to non-ASCII values. A value we cannot decode is
// returned verbatim: a slightly wrong title is better than an empty one.
func decodeMetaName(v string) string {
	v = strings.TrimSpace(v)
	if v == "" {
		return ""
	}
	dec, err := (&mime.WordDecoder{}).DecodeHeader(v)
	if err != nil || dec == "" {
		return v
	}
	return dec
}

// objectFacts are the properties of the file that was actually produced (from
// its probe), as opposed to what the sessions say.
type objectFacts struct {
	duration    time.Duration
	frames      int
	codec       string
	profile     string
	sampleRate  int
	channels    int
	transcoded  bool
	remuxFailed bool
}

// objectMetadata builds the x-amz-meta-* set of the stored object.
//
// The first four keys are merge-critical: the next session of this recording
// decides from them whether the object is its own, which parts it already
// holds and whether it may append. Storage sanitises them first and never
// drops them; everything else is diagnostics.
func (m *Manager) objectMetadata(sum objectSummary, f objectFacts) map[string]string {
	meta := map[string]string{}
	set := func(k, v string) {
		if v != "" {
			meta[k] = v
		}
	}
	setTime := func(k string, t time.Time) {
		if !t.IsZero() {
			meta[k] = t.UTC().Format(time.RFC3339)
		}
	}

	set("recording-id", sum.recordingID)
	set("sessions", strings.Join(sum.digests, ","))
	meta["parts"] = strconv.Itoa(sum.parts)
	setTime("last-session-start", sum.lastStart)

	set("name", sum.name)
	set("source", sum.source)
	setTime("scheduled-start", sum.schedStart)
	setTime("scheduled-end", sum.schedEnd)
	setTime("session-start", sum.sessStart)
	setTime("session-end", sum.sessEnd)
	// The duration of the WHOLE object (the API's per-session durationSeconds
	// stays per session).
	meta["duration-seconds"] = strconv.FormatFloat(f.duration.Seconds(), 'f', 3, 64)
	if f.frames > 0 {
		meta["frames"] = strconv.Itoa(f.frames)
	}
	set("codec", f.codec)
	set("profile", f.profile)
	if f.sampleRate > 0 {
		meta["sample-rate"] = strconv.Itoa(f.sampleRate)
	}
	if f.channels > 0 {
		meta["channels"] = strconv.Itoa(f.channels)
	}
	meta["ffmpeg-restarts"] = strconv.Itoa(sum.restarts)
	set("ffmpeg-exit-reasons", exitReasonsSummary(sum.exits))
	set("finish-reason", sum.finishReason)
	set("recorder-version", m.version)
	if sum.runs > 0 {
		meta["ffmpeg-runs"] = strconv.Itoa(sum.runs)
	}
	if !sum.firstSample.IsZero() {
		meta["first-sample-time"] = sum.firstSample.UTC().Format(milliLayout)
	}
	set("clock-source", sum.clockSource)
	if f.transcoded {
		meta["transcoded"] = "true"
		meta["stream-params-changed"] = "true"
	}
	if f.remuxFailed {
		meta["remux-failed"] = "true"
	}
	return meta
}

// mp4Metadata builds what ffmpeg embeds in the container. offset is the media
// time the new sessions start at inside the object (the duration of the part
// already stored, 0 when there is none).
func (m *Manager) mp4Metadata(sum objectSummary, toAdd []*uploadMember, remoteEntries []runEntry,
	offset time.Duration,
) remux.Metadata {
	meta := remux.Metadata{
		CreationTime: sum.firstSample,
		Title:        sum.name,
		Comment: fmt.Sprintf("spectado-stream-recorder %s; id=%s; scheduled %s–%s UTC; source=%s",
			m.version, sum.recordingID, formatOrEmpty(sum.schedStart), formatOrEmpty(sum.schedEnd), sum.source),
	}
	if meta.Title == "" {
		meta.Title = sum.recordingID
	}
	// The creation time must be the wall clock of the first sample; a recording
	// without a single run record falls back to when the capture started, and
	// only a completely unknown clock leaves the flag out.
	if meta.CreationTime.IsZero() {
		meta.CreationTime = sum.sessStart
	}
	if !sum.schedStart.IsZero() {
		loc := m.cfg.KeyDateLocation
		if loc == nil {
			loc = time.UTC
		}
		meta.Date = sum.schedStart.In(loc).Format("2006-01-02")
	}
	meta.Description = renderRunTable(m.runTable(toAdd, remoteEntries, offset))
	return meta
}

// runTable maps every ffmpeg run of the object to its position and wall clock.
// The part already stored keeps its own table verbatim in front (it describes
// bytes we are not re-deriving), and the new sessions are appended behind it.
func (m *Manager) runTable(toAdd []*uploadMember, remoteEntries []runEntry, offset time.Duration) []runEntry {
	entries := append([]runEntry(nil), remoteEntries...)
	off := offset.Seconds()

	m.mu.Lock()
	for _, mem := range toAdd {
		s := mem.s
		if len(s.Runs) == 0 {
			// A sidecar from before per-run records (or a run that never
			// persisted): the whole session is one entry anchored at its start.
			d := mem.info.Duration.Seconds()
			entries = append(entries, runEntry{SID: s.SessionID, T: s.SessionStart.UTC().Format(milliLayout),
				Src: "wallclock", Off: round3(off), Dur: round3(d)})
			off += d
			continue
		}
		for i, r := range s.Runs {
			d := r.DurationSeconds
			if i < len(mem.runs) {
				d = mem.runs[i].Duration.Seconds()
			}
			src := r.AnchorSource
			if src == "" {
				src = "wallclock"
			}
			entries = append(entries, runEntry{SID: s.SessionID, T: r.Anchor.UTC().Format(milliLayout),
				Src: src, Off: round3(off), Dur: round3(d)})
			off += d
		}
	}
	m.mu.Unlock()
	return capRunTable(entries, maxRunTableEntries)
}

// capRunTable keeps the table below max entries by coalescing the OLDEST
// entries: consecutive runs of the same session collapse into one entry that
// still spans their whole range, so the map stays continuous and only loses
// resolution at the front.
func capRunTable(entries []runEntry, max int) []runEntry {
	for len(entries) > max && len(entries) > 1 {
		last := 0
		for last+1 < len(entries) && entries[last+1].SID == entries[0].SID {
			last++
		}
		if last == 0 {
			// Already one entry per session: merge the two oldest sessions'
			// entries into the older one rather than dropping the map's head.
			last = 1
		}
		merged := entries[0]
		for _, e := range entries[1 : last+1] {
			merged.Dur += e.Dur
		}
		merged.Dur = round3(merged.Dur)
		entries = append([]runEntry{merged}, entries[last+1:]...)
	}
	return entries
}

// renderRunTable renders the table as the single-line JSON array that goes into
// the container's `description` tag. An empty table is omitted entirely.
func renderRunTable(entries []runEntry) string {
	if len(entries) == 0 {
		return ""
	}
	data, err := json.Marshal(entries)
	if err != nil {
		return ""
	}
	return string(data)
}

// parseRunTable reads the table back from a stored object's `description` tag.
// A table we cannot read is a diagnostics loss, never a reason to refuse the
// merge: the audio is what matters.
func parseRunTable(desc string, log *slog.Logger) []runEntry {
	desc = strings.TrimSpace(desc)
	if desc == "" {
		return nil
	}
	var entries []runEntry
	if err := json.Unmarshal([]byte(desc), &entries); err != nil {
		log.Warn("stored object has an unreadable run table; the merged object starts a new one", "error", err)
		return nil
	}
	return entries
}

// ---------------------------------------------------------------------------
// small helpers
// ---------------------------------------------------------------------------

func parseMetaTime(v string) time.Time {
	v = strings.TrimSpace(v)
	if v == "" {
		return time.Time{}
	}
	if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
		return t
	}
	t, err := time.Parse(time.RFC3339, v)
	if err != nil {
		return time.Time{}
	}
	return t
}

func atoiMeta(v string) int {
	n, err := strconv.Atoi(strings.TrimSpace(v))
	if err != nil {
		return 0
	}
	return n
}

func earliest(a, b time.Time) time.Time {
	if a.IsZero() || (!b.IsZero() && b.Before(a)) {
		return b
	}
	return a
}

func latest(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}
	return a
}

func formatOrEmpty(t time.Time) string {
	if t.IsZero() {
		return "?"
	}
	return t.UTC().Format(time.RFC3339)
}

// round3 keeps the run table readable: milliseconds are the precision of the
// anchors, and full float64 noise would triple the size of the tag.
func round3(v float64) float64 { return math.Round(v*1000) / 1000 }
