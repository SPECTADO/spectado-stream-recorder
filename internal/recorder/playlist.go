package recorder

import (
	"bytes"
	"context"
	"fmt"
	"path"
	"sync"
	"time"

	"github.com/spectado/stream-recorder/internal/hls"
)

const playlistContentType = "application/vnd.apple.mpegurl"

// segmentsFor describes the session's audio file as one or more playlist
// entries: one #EXT-X-BYTERANGE segment per ffmpeg run, each carrying its own
// #EXT-X-PROGRAM-DATE-TIME (the wall-clock anchor of that byte range). Runs that
// wrote no bytes are dropped. Sidecars written by older versions (no run
// records) fall back to a single whole-file segment with the wall-clock length.
func segmentsFor(s *Session) []hls.Segment {
	base := path.Base(s.Key)
	if len(s.Runs) == 0 {
		return []hls.Segment{{URI: base, Duration: wholeFileDuration(s)}}
	}
	out := make([]hls.Segment, 0, len(s.Runs))
	for _, r := range s.Runs {
		if r.Bytes == 0 {
			continue
		}
		d := r.DurationSeconds
		if d < 0 {
			d = 0
		}
		out = append(out, hls.Segment{
			URI:             base,
			Duration:        d,
			Offset:          r.Offset,
			Length:          r.Bytes,
			ProgramDateTime: r.Anchor,
		})
	}
	if len(out) == 0 {
		return []hls.Segment{{URI: base, Duration: wholeFileDuration(s)}}
	}
	return out
}

// wholeFileDuration is the fallback playback length used when no per-run data is
// available: the ADTS scan length, or the wall-clock session length.
func wholeFileDuration(s *Session) float64 {
	d := s.DurationSeconds
	if d <= 0 && s.SessionEnd != nil {
		d = s.SessionEnd.Sub(s.SessionStart).Seconds()
	}
	if d < 0 {
		d = 0
	}
	return d
}

// publishPlaylist rewrites the index.m3u8 of the folder holding s.Key so it
// lists every media file known to be there: the entries of the playlist
// currently in the bucket, the other local sessions already stored in the same
// folder, and s itself. Rewrites of one playlist are serialised: several
// sessions of the same show can upload concurrently after an outage.
func (m *Manager) publishPlaylist(ctx context.Context, s *Session) error {
	m.mu.Lock()
	pkey := playlistKey(s.Key)
	fresh := segmentsFor(s)
	for _, other := range m.sessions {
		if other == s || other.Key == "" || playlistKey(other.Key) != pkey {
			continue
		}
		if other.State == StateUploaded || other.MediaUploaded {
			fresh = append(fresh, segmentsFor(other)...)
		}
	}
	m.mu.Unlock()

	unlock := m.playlistLocks.lock(pkey)
	defer unlock()

	pctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	body, found, err := m.up.GetObject(pctx, pkey)
	if err != nil {
		return fmt.Errorf("read playlist %s: %w", pkey, err)
	}
	var existing []hls.Segment
	if found {
		existing, err = hls.Parse(body)
		if err != nil {
			m.log.Warn("existing playlist is not parsable; rewriting it", "key", pkey, "error", err)
		}
	}
	segs := hls.Merge(existing, fresh)
	data := hls.Render(segs)
	if found && bytes.Equal(data, body) {
		return nil
	}
	if err := m.up.PutObject(pctx, pkey, playlistContentType, data); err != nil {
		return fmt.Errorf("write playlist %s: %w", pkey, err)
	}
	m.log.Info("playlist updated", "id", s.ID, "session", s.SessionID, "key", pkey, "segments", len(segs))
	return nil
}

// keyedLocks hands out one mutex per key; entries disappear once unused.
type keyedLocks struct {
	mu    sync.Mutex
	locks map[string]*keyedLock
}

type keyedLock struct {
	mu   sync.Mutex
	refs int
}

// lock blocks until the key's mutex is held and returns its unlock function.
func (k *keyedLocks) lock(key string) (unlock func()) {
	k.mu.Lock()
	if k.locks == nil {
		k.locks = map[string]*keyedLock{}
	}
	l := k.locks[key]
	if l == nil {
		l = &keyedLock{}
		k.locks[key] = l
	}
	l.refs++
	k.mu.Unlock()

	l.mu.Lock()
	return func() {
		l.mu.Unlock()
		k.mu.Lock()
		l.refs--
		if l.refs == 0 {
			delete(k.locks, key)
		}
		k.mu.Unlock()
	}
}
