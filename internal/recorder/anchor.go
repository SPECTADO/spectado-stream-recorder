package recorder

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/spectado/stream-recorder/internal/hls"
)

// anchorLookupTimeout bounds the best-effort source-playlist fetch done before
// each ffmpeg run. It runs on every (re)start, so it must be short.
const anchorLookupTimeout = 3 * time.Second

// anchorBodyLimit caps how much of the source playlist is read.
const anchorBodyLimit = 1 << 20 // 1 MiB

// lookupAnchor determines the wall-clock time of the first sample of the
// upcoming ffmpeg run from the source playlist's #EXT-X-PROGRAM-DATE-TIME.
//
// It is best effort: for non-HLS sources or when CLOCK_PDT_LOOKUP is off it
// returns ("wallclock") and the writer stamps the first frame's arrival time
// instead. Any error (timeout, non-200, no PDT, unparsable) also falls back to
// "wallclock" and logs at Debug — never Warn, since this runs on every restart.
//
// The segment ffmpeg will begin with mirrors fact 0.5: on #EXT-X-ENDLIST the
// first segment; on a restart (-live_start_index -1) the newest; on a fresh
// start (ffmpeg default -3) the oldest of the last three.
func (m *Manager) lookupAnchor(ctx context.Context, s *Session, restart bool) (time.Time, string) {
	if !isHLS(s) || !m.cfg.ClockPDTLookup {
		return time.Now(), "wallclock"
	}

	debug := func(reason string, args ...any) {
		m.log.Debug("clock anchor falls back to receipt time", append([]any{"id", s.ID, "reason", reason}, args...)...)
	}

	verify := m.cfg.FFmpegTLSVerify && !s.InsecureTLS
	lp, err := m.fetchLivePlaylist(ctx, s.Source, s.Headers, verify)
	if err != nil {
		debug("fetch failed", "error", RedactURL(err.Error()))
		return time.Time{}, "wallclock"
	}
	if lp.Master {
		if lp.Variant == "" {
			debug("master playlist without a variant")
			return time.Time{}, "wallclock"
		}
		lp, err = m.fetchLivePlaylist(ctx, lp.Variant, s.Headers, verify)
		if err != nil {
			debug("variant fetch failed", "error", RedactURL(err.Error()))
			return time.Time{}, "wallclock"
		}
	}

	n := len(lp.Segments)
	if n == 0 {
		debug("no segments in playlist")
		return time.Time{}, "wallclock"
	}
	var idx int
	switch {
	case lp.EndList:
		idx = 0
	case restart:
		idx = n - 1
	default:
		idx = n - 3
		if idx < 0 {
			idx = 0
		}
	}
	pdt := lp.Segments[idx].ProgramDateTime
	if pdt.IsZero() {
		debug("no program-date-time on the starting segment", "segment", idx, "segments", n)
		return time.Time{}, "wallclock"
	}
	return pdt, "hls-pdt"
}

// fetchLivePlaylist GETs rawURL with the session headers and parses it.
func (m *Manager) fetchLivePlaylist(ctx context.Context, rawURL string, headers map[string]string, verify bool) (hls.LivePlaylist, error) {
	base, err := url.Parse(rawURL)
	if err != nil {
		return hls.LivePlaylist{}, err
	}
	lctx, cancel := context.WithTimeout(ctx, anchorLookupTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(lctx, http.MethodGet, rawURL, nil)
	if err != nil {
		return hls.LivePlaylist{}, err
	}
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	req.Header.Set("User-Agent", m.cfg.FFmpegUserAgent)

	client := &http.Client{
		Transport: &http.Transport{
			TLSClientConfig:   &tls.Config{InsecureSkipVerify: !verify}, //nolint:gosec // mirrors ffmpeg's FFMPEG_TLS_VERIFY / per-item InsecureTLS
			DisableKeepAlives: true,
			Proxy:             http.ProxyFromEnvironment,
		},
	}
	resp, err := client.Do(req)
	if err != nil {
		return hls.LivePlaylist{}, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return hls.LivePlaylist{}, fmt.Errorf("status %d", resp.StatusCode)
	}
	data, err := io.ReadAll(io.LimitReader(resp.Body, anchorBodyLimit))
	if err != nil {
		return hls.LivePlaylist{}, err
	}
	lp, err := hls.ParseLive(data, base)
	if err != nil {
		return hls.LivePlaylist{}, err
	}
	return lp, nil
}
