package recorder

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/spectado/stream-recorder/internal/adts"
	"github.com/spectado/stream-recorder/internal/remux"
	"github.com/spectado/stream-recorder/internal/schedule"
)

const (
	// objectContentType is what a recording is stored as: MP4 audio. The
	// fallback after repeated remux failures keeps the raw ADTS instead.
	objectContentType   = "audio/mp4"
	fallbackContentType = "audio/aac"

	// maxPartsPerObject bounds how many sessions one object may hold. Each part
	// costs a download+extract when the next one merges, and the `sessions`
	// manifest has to stay inside the metadata budget (20 x 9 B).
	maxPartsPerObject = 20

	// maxRemuxFailures is how often a key's remux may fail before the recording
	// is stored as raw ADTS instead. Recorded audio must never stay out of the
	// bucket forever because ffmpeg cannot digest it.
	maxRemuxFailures = 3

	// maxKeyRenames bounds the "-2", "-3" ... escape hatch (as before 1.1.0).
	maxKeyRenames = 3

	// maxImmediateRetries bounds the zero-delay retry chain. "The object moved
	// under us, look again" is normally self-limiting, but a store that keeps
	// answering the same way (a conditional PUT that always reports the object
	// exists, a rename that always lands on a taken key) would otherwise spin
	// this loop through claim/scan/remux/PUT without pause and without ever
	// counting an attempt. After this many in a row the outcome is treated like
	// any other failure: backoff, an attempt, a visible error.
	maxImmediateRetries = 3

	// keyPinWindow is how long after its scheduled end a session still lends
	// its object key to a new session of the same recording (extension after
	// the end, re-added item, rotation, a start moved across midnight).
	keyPinWindow = time.Hour

	headTimeout = 2 * time.Minute
)

// errDeferred is not a failure: the attempt did nothing because a sibling
// session of the same recording is still capturing into the same object.
var errDeferred = errors.New("upload deferred: another session of this recording is still capturing")

// errKeyChanged means the key the loop serialised on is no longer the session's
// key (a rename happened in between), so this attempt has to start over under
// the new key's lock.
var errKeyChanged = errors.New("object key changed since the attempt started")

// errKeyRenamed is returned after renameGroup moved the group to a new key; the
// loop retries immediately under that key.
var errKeyRenamed = errors.New("group moved to a new object key")

// ---------------------------------------------------------------------------
// queueing
// ---------------------------------------------------------------------------

func (m *Manager) enqueueUpload(s *Session) {
	m.mu.Lock()
	if m.shuttingDown.Load() || m.uploadLoops[s.SessionID] {
		m.mu.Unlock()
		return
	}
	m.uploadLoops[s.SessionID] = true
	m.uploadWG.Add(1)
	m.mu.Unlock()
	if m.up == nil {
		go m.keepLocally(s)
		return
	}
	go m.uploadLoop(s)
}

var (
	blockedRetryInterval = 15 * time.Minute // var so tests can shorten it

	// deferInterval is how long an upload waits while another session of the
	// same recording is still capturing: the parts are merged once, in order,
	// when the last of them is done. (var so tests can shorten it)
	deferInterval = 30 * time.Second
)

func (m *Manager) uploadLoop(s *Session) {
	defer m.uploadWG.Done()
	ctx := m.uploadCtx
	// The first retry waits 5 s (or less when the configured ceiling is lower).
	backoff := 5 * time.Second
	if m.cfg.UploadBackoffMax > 0 && backoff > m.cfg.UploadBackoffMax {
		backoff = m.cfg.UploadBackoffMax
	}
	log := m.log.With("id", s.ID, "session", s.SessionID)
	// Consecutive retries that cost no delay; reset by any other outcome.
	immediate := 0

	finish := func() {
		m.mu.Lock()
		delete(m.uploadLoops, s.SessionID)
		m.mu.Unlock()
	}
	for {
		// The key is re-read every iteration and the keyed lock is taken before
		// the worker slot: all sessions of one object are serialised against
		// each other, and a rename never leaves the loop serialised on the key
		// the group has just left.
		m.mu.Lock()
		key := s.Key
		m.mu.Unlock()
		unlock := m.keyLocks.lock(key)

		select {
		case m.uploadSem <- struct{}{}:
		case <-ctx.Done():
			unlock()
			finish()
			return
		}
		m.met.UploadsInProgress.Inc()
		err := m.uploadOnce(ctx, s, key)
		m.met.UploadsInProgress.Dec()
		<-m.uploadSem
		// Released before any sleep: a retry in 15 minutes must not keep every
		// other session of the same object waiting.
		unlock()

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
		// Outcomes that are not failures: they cost no attempt, no backoff
		// growth and no failure metric.
		switch {
		case errors.Is(err, errDeferred):
			// Nothing was done and nothing can be done while the sibling
			// records; during shutdown that sibling is being suspended, so the
			// session simply waits for the next start instead of holding the
			// process until SHUTDOWN_TIMEOUT.
			if m.shuttingDown.Load() {
				finish()
				return
			}
			immediate = 0
			log.Debug("upload deferred while a sibling session is still recording", "key", key, "retryIn", deferInterval.String())
			select {
			case <-ctx.Done():
				finish()
				return
			case <-time.After(deferInterval):
			}
			continue
		case errors.Is(err, errKeyChanged), errors.Is(err, errKeyRenamed), errors.Is(err, ErrObjectChanged), errors.Is(err, ErrObjectExists):
			// The object moved under us (another writer, a rename, a racing
			// conditional put): the next attempt HEADs it and decides again.
			immediate++
			if immediate <= maxImmediateRetries {
				log.Info("retrying upload immediately", "key", key, "reason", err)
				continue
			}
			// Not converging: fall through to the normal failure handling so the
			// loop backs off instead of burning a worker on it.
			log.Warn("upload kept restarting without making progress; backing off",
				"key", key, "reason", err, "immediateRetries", immediate-1)
		}
		immediate = 0

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

// ---------------------------------------------------------------------------
// one attempt
// ---------------------------------------------------------------------------

// uploadMember is one session of the group an attempt handles, plus what the
// attempt measured about its capture file.
type uploadMember struct {
	s      *Session
	path   string
	size   int64
	digest string
	info   adts.Info   // whole-file scan (measured members only)
	runs   []adts.Info // per-run scan, aligned with Session.Runs
}

// uploadOnce performs one attempt for the object under key: it collects every
// session of this recording that belongs there, compares them against the
// object in the bucket, remuxes what is missing into a single .m4a and stores
// it. A nil return means every session it handled reached a terminal state.
//
// The key is passed in (not read from s) because the caller serialised on it:
// if it no longer matches, the attempt would run outside its lock.
func (m *Manager) uploadOnce(ctx context.Context, s *Session, key string) error {
	group, err := m.claimGroup(s, key)
	if err != nil || len(group) == 0 {
		return err
	}
	log := m.log.With("id", s.ID, "session", s.SessionID, "key", key)
	dir := s.Dir()
	tmpID := s.SessionID

	// 1. Classify by file: an empty capture is a terminal failure, a missing
	//    file may still be covered by the object (decided by the HEAD below).
	local, missing := m.classifyMembers(group, log)

	// 2. What is in the bucket already?
	hctx, cancel := context.WithTimeout(ctx, headTimeout)
	info, found, herr := m.up.Head(hctx, key)
	cancel()
	if herr != nil {
		return fmt.Errorf("head %s: %w", key, herr)
	}

	var (
		remote  *remoteObject
		toAdd   []*uploadMember
		present []*uploadMember
	)
	if found {
		remote, err = parseRemoteObject(info)
		switch {
		case errors.Is(err, errForeignObject):
			log.Error("object under this key was not written by this recorder", "size", info.Size)
			return m.renameGroup(group, "conflict", log)
		case err != nil:
			// Fail closed: a manifest we cannot trust must never be appended to.
			log.Error("object manifest inconsistent; refusing to merge", "error", err)
			return &PermanentError{Err: fmt.Errorf("object %s: %w", key, err)}
		}
		if remote.recordingID != s.ID {
			log.Error("object under this key belongs to a different recording", "remoteId", remote.recordingID)
			return m.renameGroup(group, "conflict", log)
		}
		for _, mem := range local {
			if remote.has(mem.digest) {
				present = append(present, mem)
			} else {
				toAdd = append(toAdd, mem)
			}
		}
		for _, mem := range missing {
			if remote.has(mem.digest) {
				present = append(present, mem)
			} else {
				m.failMember(mem, "recording file missing", log)
			}
		}
		// An earlier part cannot be appended after a later one: the merged
		// timeline would run backwards.
		for _, mem := range toAdd {
			if !remote.lastStart.IsZero() && mem.s.SessionStart.Before(remote.lastStart) {
				log.Error("session is older than the last part of the object", "sessionStart", mem.s.SessionStart.UTC().Format(time.RFC3339),
					"objectLastStart", remote.lastStart.UTC().Format(time.RFC3339))
				return m.renameGroup(group, "out-of-order", log)
			}
		}
		if len(toAdd) > 0 && remote.parts+len(toAdd) > maxPartsPerObject {
			log.Error("object would exceed the parts cap", "parts", remote.parts, "adding", len(toAdd), "cap", maxPartsPerObject)
			return m.renameGroup(group, "parts-cap", log)
		}
	} else {
		toAdd = local
		for _, mem := range missing {
			m.failMember(mem, "recording file missing", log)
		}
	}

	// 3. Measure every part we are about to add (and drop the worthless ones).
	measured := make([]*uploadMember, 0, len(toAdd))
	for _, mem := range toAdd {
		if err := m.measure(mem, log); err != nil {
			return err
		}
		if mem.info.Frames == 0 {
			// Not one decodable frame: uploading it would store junk under a
			// name that promises audio.
			m.failMember(mem, "no ADTS frames found", log)
			continue
		}
		measured = append(measured, mem)
	}
	toAdd = measured

	// An empty group after classification is not an error: everything this
	// attempt was responsible for is already stored or already terminal.
	if len(toAdd) == 0 {
		if len(present) == 0 {
			return nil
		}
		return m.completeGroup(present, key, info.ETag, completion{}, log)
	}

	merging := remote != nil
	if merging {
		ok, err := m.conditionalWrites(ctx)
		if err != nil {
			return err
		}
		if !ok {
			log.Error("endpoint does not enforce conditional writes; refusing to merge into an existing object")
			return m.renameGroup(group, "no-conditional-writes", log)
		}
	}

	// 4. Disk preflight: the .m4a plus, when merging, the downloaded object and
	//    its extraction have to fit next to the captures.
	var localBytes int64
	for _, mem := range toAdd {
		localBytes += mem.size
	}
	need := localBytes*12/10 + m.cfg.MinFreeDiskBytes
	if merging {
		need += 3 * remote.size
	}
	if err := m.checkDiskFor(key, need, log); err != nil {
		return err
	}

	// 5. Build the object.
	tmps := &tempFiles{}
	defer tmps.cleanup()

	parts := make([]buildPart, 0, len(toAdd)+1)
	var remoteEntries []runEntry
	var offset time.Duration // media time the new sessions start at in the object
	if merging {
		part, entries, err := m.fetchRemotePart(ctx, key, info, dir, tmpID, tmps, log)
		if err != nil {
			return err
		}
		parts = append(parts, part)
		remoteEntries = entries
		offset = part.info.Duration
	}
	for _, mem := range toAdd {
		parts = append(parts, buildPart{path: mem.path, info: mem.info})
	}

	sum := m.summarize(toAdd, remote)
	out := filepath.Join(dir, ".tmp-upload-"+tmpID+objectExt)
	tmps.add(out)
	meta := m.mp4Metadata(sum, toAdd, remoteEntries, offset)
	res, transcoded, err := m.build(ctx, parts, out, tmpID, toAdd, meta, log)
	if err != nil {
		failures := m.noteRemuxFailure(key, group)
		m.met.RemuxTotal.WithLabelValues("failure").Inc()
		log.Error("remux failed", "error", err, "consecutiveFailures", failures)
		if failures < maxRemuxFailures {
			return fmt.Errorf("remux: %w", err)
		}
		return m.uploadFallback(ctx, group, toAdd, present, key, dir, tmpID, remote, sum, tmps, log)
	}

	// 6. Store it: a new object is written conditionally (never overwrite), a
	//    merge replaces exactly the object the HEAD saw.
	replaceETag := ""
	if merging {
		replaceETag = remote.etag
	}
	objMeta := m.objectMetadata(sum, objectFacts{
		duration:   res.Duration,
		frames:     res.Frames,
		codec:      res.Codec,
		profile:    res.Profile,
		sampleRate: res.SampleRate,
		channels:   res.Channels,
		transcoded: transcoded,
	})
	etag, err := m.putObject(ctx, out, key, objectContentType, objMeta, replaceETag, res.Size, log)
	if err != nil {
		return err
	}
	if merging {
		m.met.UploadMergesTotal.Inc()
	}

	// 7. Both the sessions that were already in the object and the ones this
	//    attempt added are done.
	return m.completeGroup(append(toAdd, present...), key, etag,
		completion{transcoded: transcoded, duration: res.Duration}, log)
}

// claimGroup collects every session that belongs in the object under key — same
// recording id, same key, waiting for or in an upload — marks them uploading
// and returns them in capture order. All of them are handled by this attempt;
// the other members' loops wake up, find their session terminal and exit.
func (m *Manager) claimGroup(s *Session, key string) ([]*Session, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if s.IsTerminal() {
		return nil, nil
	}
	if s.Key != key {
		return nil, errKeyChanged
	}
	// Defer while a sibling is still capturing into the same object: merging
	// locally at the end costs one remux instead of one per rotation, and an
	// object may only grow in capture order.
	for _, o := range m.sessions {
		if o.ID == s.ID && o.Key == key && o.State == StateRecording {
			next := time.Now().Add(deferInterval)
			s.NextUploadAt = &next
			_ = s.save()
			return nil, errDeferred
		}
	}
	var group []*Session
	for _, o := range m.sessions {
		if o.ID != s.ID || o.Key != key {
			continue
		}
		if o.State == StateFinalized || o.State == StateUploading {
			group = append(group, o)
		}
	}
	sort.Slice(group, func(i, j int) bool { return group[i].SessionStart.Before(group[j].SessionStart) })
	for _, o := range group {
		o.State = StateUploading
		o.NextUploadAt = nil
		if err := o.save(); err != nil {
			m.log.Warn("save sidecar", "id", o.ID, "session", o.SessionID, "error", err)
		}
	}
	return group, nil
}

// classifyMembers splits the group by what is on disk: an empty capture is a
// terminal failure (as before 1.1.0), a missing file is only decided once the
// object's manifest is known.
func (m *Manager) classifyMembers(group []*Session, log *slog.Logger) (local, missing []*uploadMember) {
	for _, o := range group {
		mem := &uploadMember{s: o, path: o.FilePath(), digest: sessionDigest(o.SessionID)}
		st, err := os.Stat(mem.path)
		switch {
		case err == nil && st.Size() == 0:
			m.failMember(mem, "empty recording (no data captured)", log)
		case err == nil:
			mem.size = st.Size()
			local = append(local, mem)
		default:
			missing = append(missing, mem)
		}
	}
	return local, missing
}

// failMember parks one session in the terminal failed state with a reason.
func (m *Manager) failMember(mem *uploadMember, reason string, log *slog.Logger) {
	m.mu.Lock()
	mem.s.State = StateFailed
	mem.s.LastError = reason
	mem.s.NextUploadAt = nil
	_ = mem.s.save()
	m.mu.Unlock()
	log.Error("recording will not be uploaded", "session", mem.s.SessionID, "reason", reason)
}

// measure trims a partial trailing frame and counts the frames and media time
// of every run, which is what the remux is verified against afterwards.
func (m *Manager) measure(mem *uploadMember, log *slog.Logger) error {
	if removed, err := adts.TrimPartialTail(mem.path); err != nil {
		if !errors.Is(err, os.ErrNotExist) {
			log.Warn("could not check recording tail", "session", mem.s.SessionID, "error", err)
		}
	} else if removed > 0 {
		m.mu.Lock()
		mem.s.TrimmedBytes += removed
		m.mu.Unlock()
		log.Info("trimmed partial ADTS frame before remuxing", "session", mem.s.SessionID, "bytes", removed)
	}
	st, err := os.Stat(mem.path)
	if err != nil {
		return fmt.Errorf("stat recording: %w", err)
	}
	mem.size = st.Size()

	m.mu.Lock()
	offsets := make([]int64, len(mem.s.Runs))
	for i := range mem.s.Runs {
		offsets[i] = mem.s.Runs[i].Offset
	}
	m.mu.Unlock()

	runs, total, err := scanRunsRecording(mem.path, offsets)
	if err != nil {
		return fmt.Errorf("measure %s: %w", mem.s.SessionID, err)
	}
	mem.runs, mem.info = runs, total
	if total.Junk > 0 {
		log.Warn("recording contains bytes outside ADTS frames", "session", mem.s.SessionID,
			"bytes", total.Junk, "frames", total.Frames)
	}

	m.mu.Lock()
	n := len(mem.s.Runs)
	for i := range mem.s.Runs {
		if i < len(runs) {
			mem.s.Runs[i].Frames = runs[i].Frames
			mem.s.Runs[i].DurationSeconds = runs[i].Duration.Seconds()
		}
		// Reconcile every run's byte length to its true extent in the file: the
		// gap to the next run's offset, and the last run to EOF. The sidecar
		// counter can lag the on-disk file by a persist interval, so without
		// this a run whose counters never persisted would leave bytes
		// unattributed. ScanRuns already attributed frames by these same
		// boundaries.
		want := mem.size - mem.s.Runs[i].Offset
		if i+1 < n {
			want = mem.s.Runs[i+1].Offset - mem.s.Runs[i].Offset
		}
		if want >= 0 && mem.s.Runs[i].Bytes != want {
			mem.s.Runs[i].Bytes = want
		}
	}
	mem.s.Size = mem.size
	mem.s.Bytes = mem.size
	mem.s.DurationSeconds = total.Duration.Seconds()
	_ = mem.s.save()
	m.mu.Unlock()
	return nil
}

// checkDiskFor refuses to start a remux that would fill the disk. It is a
// retryable outcome, not a remux failure: nothing about the capture is wrong.
func (m *Manager) checkDiskFor(key string, need int64, log *slog.Logger) error {
	m.mu.Lock()
	free, ok := m.diskFree()
	last, logged := m.nospaceLogged[key]
	m.mu.Unlock()
	if !ok || free >= uint64(need) {
		return nil
	}
	m.met.RemuxTotal.WithLabelValues("nospace").Inc()
	if !logged || time.Since(last) > time.Hour {
		m.mu.Lock()
		m.nospaceLogged[key] = time.Now()
		m.mu.Unlock()
		log.Warn("not enough free disk to remux this recording; will retry", "free", free, "need", need)
	}
	return fmt.Errorf("nospace: %d bytes free, %d needed to remux", free, need)
}

// ---------------------------------------------------------------------------
// remote object
// ---------------------------------------------------------------------------

// errForeignObject marks an object that this recorder did not write (no
// manifest at all): it must never be overwritten or appended to.
var errForeignObject = errors.New("object has no recorder manifest")

// remoteObject is the manifest of the object already stored under the key.
type remoteObject struct {
	etag        string
	size        int64
	meta        map[string]string
	recordingID string
	sessions    []string // session digests, in object order
	parts       int
	lastStart   time.Time
}

func (r *remoteObject) has(digest string) bool {
	for _, d := range r.sessions {
		if d == digest {
			return true
		}
	}
	return false
}

// manifestHasAll reports whether the stored object already lists every session
// this attempt wanted to add — the signature of an upload that succeeded and
// then lost its sidecar writes to a crash.
func manifestHasAll(r *remoteObject, members []*uploadMember) bool {
	for _, mem := range members {
		if !r.has(mem.digest) {
			return false
		}
	}
	return true
}

// parseRemoteObject reads the merge-critical metadata of an existing object.
// A missing manifest is a foreign object (rename), a manifest that contradicts
// itself is an error that blocks the upload instead of appending blindly.
func parseRemoteObject(info ObjectInfo) (*remoteObject, error) {
	id := strings.TrimSpace(info.Metadata["recording-id"])
	list := strings.TrimSpace(info.Metadata["sessions"])
	if id == "" || list == "" {
		return nil, errForeignObject
	}
	var digests []string
	for _, d := range strings.Split(list, ",") {
		if d = strings.TrimSpace(d); d != "" {
			digests = append(digests, d)
		}
	}
	parts, err := strconv.Atoi(strings.TrimSpace(info.Metadata["parts"]))
	if err != nil {
		return nil, fmt.Errorf("parts metadata %q is not a number", info.Metadata["parts"])
	}
	if parts != len(digests) {
		return nil, fmt.Errorf("parts=%d but the sessions manifest lists %d", parts, len(digests))
	}
	ro := &remoteObject{
		etag: info.ETag, size: info.Size, meta: info.Metadata,
		recordingID: id, sessions: digests, parts: parts,
	}
	if t, err := time.Parse(time.RFC3339, strings.TrimSpace(info.Metadata["last-session-start"])); err == nil {
		ro.lastStart = t
	}
	return ro, nil
}

// ProbeConditionalWrites runs the conditional-writes probe up front, at startup,
// instead of leaving it to the first merge: an operator has to learn that their
// endpoint silently ignores If-Match while they are still watching the log, not
// hours later when the first recording rotates. The lazy path in uploadOnce
// stays as the fallback — a probe that could not run is not cached, so the
// first merge simply asks again.
func (m *Manager) ProbeConditionalWrites(ctx context.Context) {
	if m.up == nil {
		return
	}
	if _, err := m.conditionalWrites(ctx); err != nil {
		m.log.Warn("could not probe whether the endpoint enforces conditional writes; retrying before the first merge", "error", err)
	}
}

// conditionalWrites reports (once, cached) whether the endpoint really enforces
// If-Match/If-None-Match. Merging rewrites an object that already holds audio,
// so without an enforced precondition two recorders — or two attempts — would
// silently overwrite each other's parts.
func (m *Manager) conditionalWrites(ctx context.Context) (bool, error) {
	m.mu.Lock()
	probed, allowed := m.mergeProbed, m.mergeAllowed
	m.mu.Unlock()
	if probed {
		return allowed, nil
	}
	ok, err := m.up.ConditionalWrites(ctx)
	if err != nil {
		return false, fmt.Errorf("probe conditional writes: %w", err)
	}
	m.mu.Lock()
	first := !m.mergeProbed
	m.mergeProbed, m.mergeAllowed = true, ok
	m.mu.Unlock()
	if first {
		if ok {
			m.log.Info("endpoint enforces conditional writes; sessions of one recording merge into a single object")
		} else {
			m.log.Warn("endpoint does not enforce conditional writes; later sessions of a recording get their own object")
		}
	}
	return ok, nil
}

// fetchRemotePart downloads the stored object, extracts its raw ADTS and
// verifies the extraction against the object's own sample table. Everything
// that follows treats it as the first part of the merged recording.
func (m *Manager) fetchRemotePart(ctx context.Context, key string, info ObjectInfo, dir, tmpID string,
	tmps *tempFiles, log *slog.Logger,
) (buildPart, []runEntry, error) {
	m4a := filepath.Join(dir, ".tmp-remote-"+tmpID+objectExt)
	aac := filepath.Join(dir, ".tmp-remote-"+tmpID+".aac")
	tmps.add(m4a, aac)

	dctx, cancel := context.WithTimeout(ctx, m.transferDeadline(info.Size))
	defer cancel()
	// If-Match: the object must still be the one the HEAD classified, otherwise
	// the merge would be built on a manifest that no longer applies.
	if err := m.up.Download(dctx, key, info.ETag, m4a); err != nil {
		return buildPart{}, nil, err
	}
	probe, err := m.remuxer().Probe(dctx, m4a)
	if err != nil {
		return buildPart{}, nil, fmt.Errorf("probe stored object: %w", err)
	}
	if err := m.remuxer().Extract(dctx, m4a, aac); err != nil {
		return buildPart{}, nil, fmt.Errorf("extract stored object: %w", err)
	}
	scan, err := scanRecording(aac)
	if err != nil {
		return buildPart{}, nil, fmt.Errorf("measure extracted object: %w", err)
	}
	// nb_frames comes from the MP4 sample table, so it must match the ADTS
	// exactly; 0 means ffprobe could not tell, and only the duration is left to
	// compare. A mismatch means the extraction lost audio: keep everything.
	if probe.Frames > 0 && scan.Frames != probe.Frames {
		return buildPart{}, nil, fmt.Errorf("extracted object has %d frames, the object says %d", scan.Frames, probe.Frames)
	}
	if tol := 3 * frameDuration(scan.Params); absDuration(scan.Duration-probe.Duration) > tol {
		return buildPart{}, nil, fmt.Errorf("extracted object is %s, the object says %s (tolerance %s)",
			scan.Duration, probe.Duration, tol)
	}
	entries := parseRunTable(probe.Tags["description"], log)
	log.Info("merging into the stored object", "remoteBytes", info.Size, "remoteFrames", scan.Frames,
		"remoteDuration", scan.Duration.Truncate(time.Millisecond).String())
	return buildPart{path: aac, info: scan}, entries, nil
}

// ---------------------------------------------------------------------------
// building
// ---------------------------------------------------------------------------

// buildPart is one ADTS input of the merged object, in playback order.
type buildPart struct {
	path string
	info adts.Info
}

// build produces the .m4a. A stream copy is only correct while one MP4 sample
// description can describe the whole input; when the captured parameters
// changed (encoder failover at the origin) or a frame carries several raw data
// blocks, the homogeneous chunks are re-encoded through the concat filter
// instead — a copy would silently yield a short, pitched or one-eared file.
func (m *Manager) build(ctx context.Context, parts []buildPart, out, tmpID string, toAdd []*uploadMember,
	meta remux.Metadata, log *slog.Logger,
) (remux.Result, bool, error) {
	var frames int
	var duration time.Duration
	uniform := true
	first := parts[0].info.Params
	for _, p := range parts {
		frames += p.info.Frames
		duration += p.info.Duration
		if p.info.ParamsChanged || p.info.Params != first || p.info.Params.Blocks != 1 {
			uniform = false
		}
	}

	started := time.Now()
	if uniform {
		paths := make([]string, len(parts))
		for i, p := range parts {
			paths[i] = p.path
		}
		res, err := m.remuxer().Build(ctx, paths, out, remux.Expect{Frames: frames, Duration: duration, Params: first}, meta)
		if err != nil {
			return remux.Result{}, false, err
		}
		m.met.RemuxTotal.WithLabelValues("success").Inc()
		m.met.RemuxDuration.Observe(time.Since(started).Seconds())
		log.Info("remuxed recording", "frames", res.Frames, "duration", res.Duration.Truncate(time.Millisecond).String(),
			"bytes", res.Size, "took", time.Since(started).Truncate(time.Millisecond).String())
		return res, false, nil
	}

	log.Warn("captured stream parameters are not uniform; re-encoding instead of copying",
		"parameters", describeParams(parts))
	chunks, cleanup, err := writeChunks(parts, filepath.Dir(out), tmpID)
	defer cleanup()
	if err != nil {
		return remux.Result{}, false, err
	}
	bitrate := m.cfg.AudioBitrate
	if len(toAdd) > 0 {
		m.mu.Lock()
		if b := toAdd[len(toAdd)-1].s.Bitrate; b != "" {
			bitrate = b
		}
		m.mu.Unlock()
	}
	res, err := m.remuxer().BuildTranscode(ctx, chunks, out, duration, bitrate, meta)
	if err != nil {
		return remux.Result{}, false, err
	}
	m.met.RemuxTotal.WithLabelValues("success").Inc()
	m.met.RemuxTranscodedTotal.Inc()
	m.met.RemuxDuration.Observe(time.Since(started).Seconds())
	log.Info("remuxed recording (re-encoded)", "duration", res.Duration.Truncate(time.Millisecond).String(),
		"bytes", res.Size, "bitrate", bitrate, "took", time.Since(started).Truncate(time.Millisecond).String())
	return res, true, nil
}

// writeChunks materialises every homogeneous byte range of every part as its
// own file: BuildTranscode needs one ffmpeg input per chunk.
func writeChunks(parts []buildPart, dir, tmpID string) ([]remux.Chunk, func(), error) {
	var out []remux.Chunk
	var created []string
	cleanup := func() {
		for _, p := range created {
			_ = os.Remove(p)
		}
	}
	n := 0
	for _, p := range parts {
		chunks := p.info.Chunks
		if len(chunks) == 0 {
			continue
		}
		for _, c := range chunks {
			path := filepath.Join(dir, fmt.Sprintf(".tmp-chunk-%s-%d.aac", tmpID, n))
			n++
			if err := copyRange(p.path, path, c.Offset, c.Length); err != nil {
				return nil, cleanup, err
			}
			created = append(created, path)
			out = append(out, remux.Chunk{Path: path, Params: c.Params})
		}
	}
	if len(out) == 0 {
		return nil, cleanup, errors.New("no chunks to transcode")
	}
	return out, cleanup, nil
}

// copyRange writes [offset, offset+length) of src to dst.
func copyRange(src, dst string, offset, length int64) error {
	in, err := os.Open(src)
	if err != nil {
		return fmt.Errorf("open chunk source: %w", err)
	}
	defer in.Close()
	f, err := os.OpenFile(dst, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return fmt.Errorf("create chunk file: %w", err)
	}
	defer f.Close()
	if _, err := io.Copy(f, io.NewSectionReader(in, offset, length)); err != nil {
		return fmt.Errorf("write chunk file: %w", err)
	}
	return f.Close()
}

// describeParams renders the parameter sets of the parts for the log line that
// explains why a re-encode was necessary.
func describeParams(parts []buildPart) string {
	var seen []string
	add := func(p adts.Params) {
		s := fmt.Sprintf("%s/%dHz/%dch/%drdb", p.ProfileName(), p.SampleRate, p.Channels(), p.Blocks)
		for _, e := range seen {
			if e == s {
				return
			}
		}
		seen = append(seen, s)
	}
	for _, p := range parts {
		if len(p.info.Chunks) == 0 {
			add(p.info.Params)
			continue
		}
		for _, c := range p.info.Chunks {
			add(c.Params)
		}
	}
	return strings.Join(seen, " -> ")
}

// forgetKey drops the per-key bookkeeping (consecutive remux failures, the
// rate-limited disk warning) of a key this recording is done with.
func (m *Manager) forgetKey(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.remuxFailures, key)
	delete(m.nospaceLogged, key)
}

// noteRemuxFailure counts a consecutive remux failure for the key and mirrors
// the count into every member for the API.
func (m *Manager) noteRemuxFailure(key string, group []*Session) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.remuxFailures[key]++
	n := m.remuxFailures[key]
	for _, o := range group {
		o.RemuxFailures = n
	}
	return n
}

// uploadFallback stores the raw ADTS after maxRemuxFailures remuxes in a row
// failed for this key. Recorded audio must never stay out of the bucket
// forever; a clearly marked .aac next to the other objects can be remuxed by
// hand. It never merges: an object that already holds audio keeps it.
func (m *Manager) uploadFallback(ctx context.Context, group []*Session, toAdd, present []*uploadMember,
	key, dir, tmpID string, remote *remoteObject, sum objectSummary, tmps *tempFiles, log *slog.Logger,
) error {
	if remote != nil {
		log.Error("cannot store the raw capture next to an existing object; using a new key")
		return m.renameGroup(group, "remux-failed", log)
	}
	paths := make([]string, len(toAdd))
	var frames int
	var duration time.Duration
	for i, mem := range toAdd {
		paths[i] = mem.path
		frames += mem.info.Frames
		duration += mem.info.Duration
	}

	// The fallback key is an object key like any other and has to be classified
	// like the .m4a before anything is written to it. Storing it blind would
	// overwrite an earlier session's raw capture whenever the endpoint does not
	// enforce conditional writes, and would bounce on ErrObjectExists forever
	// when it does — the fallback never merges, so the only safe answers are
	// "write it", "it is already there" and "use another key".
	fbKey := strings.TrimSuffix(key, objectExt) + ".aac"
	hctx, cancel := context.WithTimeout(ctx, headTimeout)
	info, found, err := m.up.Head(hctx, fbKey)
	cancel()
	if err != nil {
		return fmt.Errorf("head %s: %w", fbKey, err)
	}
	if found {
		stored, perr := parseRemoteObject(info)
		if perr != nil || stored.recordingID != sum.recordingID || !manifestHasAll(stored, toAdd) {
			log.Error("the fallback key already holds an object this recording may not replace; using a new key",
				"key", fbKey, "error", perr)
			return m.renameGroup(group, "remux-failed", log)
		}
		// Our own captures are already there: a previous attempt stored them and
		// died before its sidecars said so. Converge instead of re-uploading.
		log.Warn("the raw capture is already stored under the fallback key; completing without uploading", "key", fbKey)
		m.forgetKey(key)
		return m.completeGroup(append(toAdd, present...), fbKey, info.ETag, completion{duration: duration}, log)
	}

	out := filepath.Join(dir, ".tmp-upload-"+tmpID+".aac")
	tmps.add(out)
	size, err := remux.Concat(paths, out)
	if err != nil {
		return fmt.Errorf("concatenate raw capture: %w", err)
	}
	objMeta := m.objectMetadata(sum, objectFacts{
		duration:    duration,
		frames:      frames,
		codec:       "aac",
		profile:     toAdd[0].info.Params.ProfileName(),
		sampleRate:  toAdd[0].info.Params.SampleRate,
		channels:    toAdd[0].info.Params.Channels(),
		remuxFailed: true,
	})
	log.Error("storing the raw ADTS capture instead of an .m4a after repeated remux failures",
		"key", fbKey, "bytes", size)
	etag, err := m.putObject(ctx, out, fbKey, fallbackContentType, objMeta, "", size, log)
	if err != nil {
		return err
	}
	m.met.UploadFallbackTotal.Inc()
	// The .m4a key is not used by this recording any more; its failure count
	// must not greet the next recording that computes the same key.
	m.forgetKey(key)
	return m.completeGroup(append(toAdd, present...), fbKey, etag, completion{duration: duration}, log)
}

// ---------------------------------------------------------------------------
// storing and completing
// ---------------------------------------------------------------------------

// putObject uploads the built file and, for a replacing upload, re-reads the
// object afterwards: a store that accepted the request but kept the old bytes
// would otherwise let the local captures be deleted.
func (m *Manager) putObject(ctx context.Context, path, key, contentType string, meta map[string]string,
	replaceETag string, size int64, log *slog.Logger,
) (string, error) {
	uctx, cancel := context.WithTimeout(ctx, m.transferDeadline(size))
	defer cancel()

	log.Info("upload started", "key", key, "bytes", size, "replace", replaceETag != "")
	t0 := time.Now()
	etag, err := m.up.Upload(uctx, path, key, contentType, meta, replaceETag)
	if err != nil {
		return "", err
	}
	took := time.Since(t0)
	if replaceETag != "" {
		info, found, err := m.up.Head(uctx, key)
		if err != nil {
			return "", fmt.Errorf("verify replaced object: %w", err)
		}
		if !found || info.Size != size || (etag != "" && info.ETag != etag) {
			return "", fmt.Errorf("replaced object does not match the upload: found=%v size=%d (want %d) etag=%q (want %q)",
				found, info.Size, size, info.ETag, etag)
		}
	}
	m.met.UploadBytesTotal.Add(float64(size))
	m.met.UploadDuration.Observe(took.Seconds())
	log.Info("object stored", "key", key, "bytes", size, "etag", etag,
		"duration", took.Truncate(time.Millisecond).String())
	return etag, nil
}

// completion carries what the finished object says about its members.
type completion struct {
	transcoded bool
	duration   time.Duration
}

// completeGroup marks every session of the object uploaded and removes its
// capture. Order matters: the sidecars are fsynced first and a file is only
// deleted once its own sidecar durably says "uploaded", so a crash in the
// middle can only cost a re-verified upload, never the audio.
func (m *Manager) completeGroup(members []*uploadMember, key, etag string, c completion, log *slog.Logger) error {
	now := time.Now()
	type write struct {
		mem  *uploadMember
		path string
		data []byte
	}
	writes := make([]write, 0, len(members))

	m.mu.Lock()
	for _, mem := range members {
		s := mem.s
		s.setKey(key)
		s.MediaUploaded = true
		s.State = StateUploaded
		s.UploadedAt = &now
		s.UploadETag = etag
		s.UploadAttempts++
		s.UploadBlocked = false
		s.LastError = ""
		s.NextUploadAt = nil
		s.RemuxFailures = 0
		if c.transcoded {
			s.Transcoded = true
		}
		data, err := s.encode()
		if err != nil {
			m.mu.Unlock()
			return fmt.Errorf("encode sidecar: %w", err)
		}
		writes = append(writes, write{mem: mem, path: s.SidecarPath(), data: data})
	}
	m.mu.Unlock()
	m.forgetKey(key)

	var failed error
	for _, w := range writes {
		if err := schedule.WriteFileAtomic(w.path, w.data, 0o644); err != nil {
			// Never delete a capture while its sidecar still says "uploading":
			// the next attempt finds the session in the object's manifest and
			// converges without uploading anything again.
			failed = fmt.Errorf("record upload in sidecar: %w", err)
			log.Error("could not record the upload in the sidecar; keeping the capture", "session", w.mem.s.SessionID, "error", err)
			continue
		}
		if err := os.Remove(w.mem.path); err != nil && !errors.Is(err, os.ErrNotExist) {
			log.Warn("remove uploaded capture (retried on next start)", "file", w.mem.path, "error", err)
		}
	}
	if failed != nil {
		return failed
	}
	m.met.UploadsTotal.WithLabelValues("success").Inc()
	log.Info("upload finished", "key", key, "sessions", len(members),
		"duration", c.duration.Truncate(time.Millisecond).String())
	return nil
}

// renameGroup moves every session of the group to the next free key ("-2",
// "-3", ...) and asks the caller to retry there. It is always a loud event: the
// recording ends up in two objects, which someone has to look at.
func (m *Manager) renameGroup(group []*Session, reason string, log *slog.Logger) error {
	m.mu.Lock()
	renames := 0
	for _, o := range group {
		if o.KeyRenames > renames {
			renames = o.KeyRenames
		}
	}
	if renames >= maxKeyRenames {
		m.mu.Unlock()
		return &PermanentError{Err: fmt.Errorf("object key still conflicts after %d renames (%s)", renames, reason)}
	}
	old := group[0].Key
	newKey := withSuffix(old, "-"+strconv.Itoa(renames+2))
	for _, o := range group {
		o.setKey(newKey)
		o.KeyRenames = renames + 1
		o.State = StateFinalized
		o.UploadAttempts++
		_ = o.save()
	}
	m.mu.Unlock()
	m.forgetKey(old)
	m.met.ObjectKeyRenamesTotal.WithLabelValues(reason).Inc()
	log.Error("recording written to a second object", "reason", reason, "oldKey", old, "newKey", newKey)
	return fmt.Errorf("%w: %s -> %s (%s)", errKeyRenamed, old, newKey, reason)
}

// ---------------------------------------------------------------------------
// kept mode
// ---------------------------------------------------------------------------

// keepLocally is the UPLOAD_DISABLED path: the capture is remuxed to
// <session>.m4a next to its sidecar and the .aac is removed only once the
// sidecar durably names the .m4a. Kept mode never merges — each session is its
// own file, exactly as it was captured.
func (m *Manager) keepLocally(s *Session) {
	defer m.uploadWG.Done()
	defer func() {
		m.mu.Lock()
		delete(m.uploadLoops, s.SessionID)
		m.mu.Unlock()
	}()
	ctx := m.uploadCtx
	select {
	case m.uploadSem <- struct{}{}:
	case <-ctx.Done():
		return // still finalized: the next start queues it again
	}
	defer func() { <-m.uploadSem }()

	log := m.log.With("id", s.ID, "session", s.SessionID)
	out := s.OutputPath()
	tmp := filepath.Join(s.Dir(), ".tmp-kept-"+s.SessionID+objectExt)
	defer os.Remove(tmp)

	fail := func(reason string, err error) {
		m.met.RemuxTotal.WithLabelValues("failure").Inc()
		m.mu.Lock()
		s.State = StateKept
		s.LastError = reason
		_ = s.save()
		m.mu.Unlock()
		log.Error("could not remux the kept recording; the raw capture stays on disk", "file", s.FilePath(), "error", err)
	}

	mem := &uploadMember{s: s, path: s.FilePath(), digest: sessionDigest(s.SessionID)}
	st, err := os.Stat(mem.path)
	switch {
	case errors.Is(err, os.ErrNotExist):
		// Already remuxed by an earlier run (crash between rename and sidecar).
		m.mu.Lock()
		s.State = StateKept
		if _, serr := os.Stat(out); serr == nil {
			s.OutputFile = out
			s.LastError = ""
		}
		_ = s.save()
		m.mu.Unlock()
		return
	case err != nil:
		fail("stat recording: "+err.Error(), err)
		return
	case st.Size() == 0:
		m.failMember(mem, "empty recording (no data captured)", log)
		return
	}
	if err := m.measure(mem, log); err != nil {
		fail("measure recording: "+err.Error(), err)
		return
	}
	if mem.info.Frames == 0 {
		m.failMember(mem, "no ADTS frames found", log)
		return
	}

	parts := []buildPart{{path: mem.path, info: mem.info}}
	members := []*uploadMember{mem}
	meta := m.mp4Metadata(m.summarize(members, nil), members, nil, 0)
	res, transcoded, err := m.build(ctx, parts, tmp, s.SessionID, members, meta, log)
	if err != nil {
		fail("remux: "+err.Error(), err)
		return
	}
	if err := os.Rename(tmp, out); err != nil {
		fail("rename remuxed file: "+err.Error(), err)
		return
	}

	m.mu.Lock()
	s.State = StateKept
	s.OutputFile = out
	s.Transcoded = transcoded
	s.LastError = ""
	saveErr := s.save()
	m.mu.Unlock()
	if saveErr != nil {
		// The .aac must outlive a sidecar that does not know about the .m4a yet.
		log.Error("could not record the kept file in the sidecar; keeping the capture", "error", saveErr)
		return
	}
	if err := os.Remove(mem.path); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Warn("remove capture after remux", "file", mem.path, "error", err)
	}
	log.Info("uploads disabled: recording remuxed and kept locally", "file", out,
		"duration", res.Duration.Truncate(time.Millisecond).String(), "transcoded", transcoded)
}

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

// transferDeadline bounds one transfer: 10 minutes plus the time the payload
// needs at the assumed minimum throughput (UPLOAD_MIN_THROUGHPUT).
func (m *Manager) transferDeadline(size int64) time.Duration {
	rate := m.cfg.UploadMinThroughput
	if rate <= 0 {
		rate = 128 * 1024
	}
	if size < 0 {
		size = 0
	}
	return 10*time.Minute + time.Duration(size/rate)*time.Second
}

// tempFiles collects the working files of one attempt so a failure anywhere
// leaves nothing behind (scanSessions would reap them after a crash, but a
// retry loop must not fill the disk in the meantime).
type tempFiles struct{ paths []string }

func (t *tempFiles) add(paths ...string) { t.paths = append(t.paths, paths...) }

func (t *tempFiles) cleanup() {
	for _, p := range t.paths {
		_ = os.Remove(p)
		_ = os.Remove(p + ".part")
	}
}

// scanRecording measures the file with adts.Scan, converting a panic in the
// parser into an error: a malformed recording must never crash the process
// (and with it every live recording) in an upload retry loop.
func scanRecording(path string) (info adts.Info, err error) {
	defer func() {
		if r := recover(); r != nil {
			info, err = adts.Info{}, fmt.Errorf("adts scan panicked: %v", r)
		}
	}()
	return adts.Scan(path)
}

// scanRunsRecording is scanRecording for the per-run byte ranges, with the same
// panic-to-error guard.
func scanRunsRecording(path string, offsets []int64) (runs []adts.Info, total adts.Info, err error) {
	defer func() {
		if r := recover(); r != nil {
			runs, total, err = nil, adts.Info{}, fmt.Errorf("adts scanRuns panicked: %v", r)
		}
	}()
	return adts.ScanRuns(path, offsets)
}

// frameDuration is the media time of one ADTS frame with these parameters.
func frameDuration(p adts.Params) time.Duration {
	if p.SampleRate <= 0 {
		return 25 * time.Millisecond
	}
	blocks := p.Blocks
	if blocks <= 0 {
		blocks = 1
	}
	return time.Duration(1024*blocks) * time.Second / time.Duration(p.SampleRate)
}

func absDuration(d time.Duration) time.Duration {
	if d < 0 {
		return -d
	}
	return d
}
