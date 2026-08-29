// Package schedule fetches and parses the recording schedule (a JSON document
// listing streams with start/end times) and keeps a last-known-good copy.
package schedule

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Duration is a time.Duration that marshals as a string ("15s").
type Duration time.Duration

// D returns the plain time.Duration.
func (d Duration) D() time.Duration { return time.Duration(d) }

// MarshalJSON implements json.Marshaler.
func (d Duration) MarshalJSON() ([]byte, error) { return json.Marshal(time.Duration(d).String()) }

// UnmarshalJSON accepts "15s" style strings or plain numbers (seconds).
func (d *Duration) UnmarshalJSON(b []byte) error {
	var n float64
	if err := json.Unmarshal(b, &n); err == nil {
		*d = Duration(time.Duration(n * float64(time.Second)))
		return nil
	}
	var s string
	if err := json.Unmarshal(b, &s); err != nil {
		return errors.New("must be a duration string or a number of seconds")
	}
	s = strings.TrimSpace(s)
	if f, err := strconv.ParseFloat(s, 64); err == nil {
		*d = Duration(time.Duration(f * float64(time.Second)))
		return nil
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration %q", s)
	}
	*d = Duration(v)
	return nil
}

// Item is one normalized schedule entry.
type Item struct {
	ID           string            `json:"id"`
	Name         string            `json:"name,omitempty"`
	Source       string            `json:"source"`
	Type         string            `json:"type,omitempty"` // hls | icecast | auto (informational)
	Start        time.Time         `json:"start"`
	End          time.Time         `json:"end"`
	Key          string            `json:"key,omitempty"`     // explicit object key (optional)
	Codec        string            `json:"codec,omitempty"`   // auto | aac | copy
	Bitrate      string            `json:"bitrate,omitempty"` // e.g. 128k
	Headers      map[string]string `json:"headers,omitempty"`
	StartEarly   *Duration         `json:"startEarly,omitempty"`   // per-item override
	StopLate     *Duration         `json:"stopLate,omitempty"`     // per-item override
	StallTimeout *Duration         `json:"stallTimeout,omitempty"` // per-item override (>= 5s)
	InsecureTLS  bool              `json:"insecureTLS,omitempty"`  // skip TLS verification for this source
}

// InvalidItem describes an entry that was rejected during parsing.
type InvalidItem struct {
	Index  int    `json:"index"`
	ID     string `json:"id,omitempty"`
	Reason string `json:"reason"`
}

// Schedule is a parsed schedule document.
type Schedule struct {
	Items      []Item        `json:"items"`
	Invalid    []InvalidItem `json:"invalid,omitempty"`
	FetchedAt  time.Time     `json:"fetchedAt"`
	ServerDate time.Time     `json:"serverDate,omitempty"` // HTTP Date header, for clock-skew checks
	ETag       string        `json:"etag,omitempty"`
	Origin     string        `json:"origin"` // url | cache
}

// Get returns the (valid) item with the given id.
func (s *Schedule) Get(id string) (Item, bool) {
	if s == nil {
		return Item{}, false
	}
	for _, it := range s.Items {
		if it.ID == id {
			return it, true
		}
	}
	return Item{}, false
}

// Present reports whether an entry with this id appears in the document at
// all — valid or rejected. A recording must only be stopped as "removed" when
// its id is truly absent, never because a later edit made the entry invalid.
func (s *Schedule) Present(id string) bool {
	if s == nil {
		return false
	}
	if _, ok := s.Get(id); ok {
		return true
	}
	for _, inv := range s.Invalid {
		if inv.ID != "" && inv.ID == id {
			return true
		}
	}
	return false
}

// InvalidReason returns the rejection reason for an id present but invalid.
func (s *Schedule) InvalidReason(id string) string {
	if s == nil {
		return ""
	}
	for _, inv := range s.Invalid {
		if inv.ID != "" && inv.ID == id {
			return inv.Reason
		}
	}
	return ""
}

// MaxDocumentSize bounds the accepted schedule document size.
const MaxDocumentSize = 16 << 20

// Parse parses a schedule document. It accepts either a top-level JSON array
// of items or an object with an "items" (or "streams"/"recordings") array.
// Invalid entries are reported in Schedule.Invalid; valid ones are kept.
// Timestamps without an offset are interpreted in loc (UTC when nil).
//
// A document whose entries are ALL invalid is treated as an error, so a
// schema change upstream cannot wipe out every running recording.
func Parse(data []byte, loc *time.Location) (*Schedule, error) {
	if loc == nil {
		loc = time.UTC
	}
	data = bytes.TrimSpace(data)
	if len(data) == 0 {
		return nil, errors.New("empty document")
	}
	var rawItems []json.RawMessage
	if data[0] == '[' {
		if err := json.Unmarshal(data, &rawItems); err != nil {
			return nil, fmt.Errorf("invalid JSON array: %w", err)
		}
	} else {
		var doc map[string]json.RawMessage
		if err := json.Unmarshal(data, &doc); err != nil {
			return nil, fmt.Errorf("invalid JSON document: %w", err)
		}
		found := false
		for _, k := range []string{"items", "streams", "recordings"} {
			if raw, ok := doc[k]; ok && string(raw) != "null" {
				if err := json.Unmarshal(raw, &rawItems); err != nil {
					return nil, fmt.Errorf("field %q is not an array: %w", k, err)
				}
				found = true
				break
			}
		}
		if !found {
			return nil, errors.New(`document must be an array or an object with an "items" array`)
		}
	}

	s := &Schedule{Items: make([]Item, 0, len(rawItems))}
	seenIDs := make(map[string]struct{}, len(rawItems))
	seenKeys := make(map[string]string, len(rawItems))
	for i, raw := range rawItems {
		it, err := parseItem(raw, loc)
		if err != nil {
			s.Invalid = append(s.Invalid, InvalidItem{Index: i, ID: it.ID, Reason: err.Error()})
			continue
		}
		if _, dup := seenIDs[it.ID]; dup {
			s.Invalid = append(s.Invalid, InvalidItem{Index: i, ID: it.ID, Reason: "duplicate id"})
			continue
		}
		if it.Key != "" {
			if other, dup := seenKeys[it.Key]; dup {
				s.Invalid = append(s.Invalid, InvalidItem{Index: i, ID: it.ID, Reason: fmt.Sprintf("key %q already used by item %q", it.Key, other)})
				continue
			}
			seenKeys[it.Key] = it.ID
		}
		seenIDs[it.ID] = struct{}{}
		s.Items = append(s.Items, it)
	}
	if len(rawItems) > 0 && len(s.Items) == 0 {
		return nil, fmt.Errorf("all %d items are invalid (first: %s)", len(rawItems), s.Invalid[0].Reason)
	}
	sort.SliceStable(s.Items, func(a, b int) bool { return s.Items[a].Start.Before(s.Items[b].Start) })
	return s, nil
}

func parseItem(raw json.RawMessage, loc *time.Location) (Item, error) {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return Item{}, fmt.Errorf("item is not an object: %w", err)
	}
	get := func(keys ...string) (json.RawMessage, bool) {
		for _, k := range keys {
			if v, ok := fields[k]; ok && string(v) != "null" {
				return v, true
			}
		}
		return nil, false
	}
	var it Item
	var err error

	// id may be a string or a number.
	if raw, ok := get("id"); ok {
		var sid string
		if json.Unmarshal(raw, &sid) != nil {
			var nid json.Number
			dec := json.NewDecoder(bytes.NewReader(raw))
			dec.UseNumber()
			if dec.Decode(&nid) != nil {
				return it, errors.New("id must be a string or number")
			}
			sid = nid.String()
		}
		it.ID = strings.TrimSpace(sid)
	}
	if it.ID == "" {
		return it, errors.New("id is required")
	}
	if len(it.ID) > 128 {
		return it, errors.New("id is longer than 128 characters")
	}
	if it.Name, err = stringField(get("name", "title")); err != nil {
		return it, fmt.Errorf("name: %w", err)
	}
	if it.Source, err = stringField(get("source", "url", "stream")); err != nil {
		return it, fmt.Errorf("source: %w", err)
	}
	it.Source = strings.TrimSpace(it.Source)
	if it.Source == "" {
		return it, errors.New("source is required")
	}
	u, err := url.Parse(it.Source)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return it, errors.New("source must be an http(s) URL")
	}
	if it.Type, err = stringField(get("type")); err != nil {
		return it, fmt.Errorf("type: %w", err)
	}
	it.Type = strings.ToLower(strings.TrimSpace(it.Type))
	switch it.Type {
	case "", "auto", "hls", "icecast":
	default:
		return it, fmt.Errorf("type must be hls, icecast or auto (got %q)", it.Type)
	}
	startRaw, ok := get("start", "startTime", "start_time", "from")
	if !ok {
		return it, errors.New("start is required")
	}
	if it.Start, err = parseTime(startRaw, loc); err != nil {
		return it, fmt.Errorf("start: %w", err)
	}
	endRaw, ok := get("end", "endTime", "end_time", "to")
	if !ok {
		return it, errors.New("end is required")
	}
	if it.End, err = parseTime(endRaw, loc); err != nil {
		return it, fmt.Errorf("end: %w", err)
	}
	if !it.End.After(it.Start) {
		return it, errors.New("end must be after start")
	}
	if it.Key, err = stringField(get("key", "objectKey", "object_key")); err != nil {
		return it, fmt.Errorf("key: %w", err)
	}
	it.Key = strings.TrimPrefix(strings.TrimSpace(it.Key), "/")
	if strings.Contains(it.Key, "..") || strings.ContainsAny(it.Key, "\r\n") {
		return it, errors.New("key must not contain '..' or line breaks")
	}
	if len(it.Key) > 900 {
		return it, errors.New("key is longer than 900 bytes")
	}
	if it.Codec, err = stringField(get("codec")); err != nil {
		return it, fmt.Errorf("codec: %w", err)
	}
	it.Codec = strings.ToLower(strings.TrimSpace(it.Codec))
	switch it.Codec {
	case "", "auto", "aac", "copy":
	default:
		return it, fmt.Errorf("codec must be auto, aac or copy (got %q)", it.Codec)
	}
	if raw, ok := get("bitrate"); ok {
		var s string
		if json.Unmarshal(raw, &s) == nil {
			it.Bitrate = strings.TrimSpace(s)
		} else {
			var n float64
			if json.Unmarshal(raw, &n) != nil || n <= 0 {
				return it, errors.New("bitrate must be a string like \"128k\" or a positive number")
			}
			it.Bitrate = strconv.FormatInt(int64(n), 10)
		}
		if it.Bitrate != "" && !validBitrate(it.Bitrate) {
			return it, fmt.Errorf("bitrate %q is not a valid ffmpeg bitrate", it.Bitrate)
		}
	}
	if hdr, ok := get("headers"); ok {
		if err := json.Unmarshal(hdr, &it.Headers); err != nil {
			return it, fmt.Errorf("headers must be an object of strings: %w", err)
		}
		for k, v := range it.Headers {
			if strings.TrimSpace(k) == "" || strings.ContainsAny(k, "\r\n:") || strings.ContainsAny(v, "\r\n") {
				return it, fmt.Errorf("headers: invalid header %q", k)
			}
		}
	}
	for _, f := range []struct {
		keys []string
		dst  **Duration
		name string
	}{
		{[]string{"startEarly", "start_early"}, &it.StartEarly, "startEarly"},
		{[]string{"stopLate", "stop_late"}, &it.StopLate, "stopLate"},
		{[]string{"stallTimeout", "stall_timeout"}, &it.StallTimeout, "stallTimeout"},
	} {
		if raw, ok := get(f.keys...); ok {
			var d Duration
			if err := json.Unmarshal(raw, &d); err != nil {
				return it, fmt.Errorf("%s: %w", f.name, err)
			}
			if d < 0 || d > Duration(6*time.Hour) {
				return it, fmt.Errorf("%s must be between 0 and 6h", f.name)
			}
			if f.name == "stallTimeout" && d < Duration(5*time.Second) {
				return it, errors.New("stallTimeout must be at least 5s")
			}
			*f.dst = &d
		}
	}
	if raw, ok := get("insecureTLS", "insecure_tls"); ok {
		if err := json.Unmarshal(raw, &it.InsecureTLS); err != nil {
			return it, errors.New("insecureTLS must be a boolean")
		}
	}
	return it, nil
}

func validBitrate(s string) bool {
	s = strings.ToLower(s)
	s = strings.TrimSuffix(s, "k")
	s = strings.TrimSuffix(s, "m")
	n, err := strconv.ParseFloat(s, 64)
	return err == nil && n > 0
}

func stringField(raw json.RawMessage, ok bool) (string, error) {
	if !ok {
		return "", nil
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return "", errors.New("must be a string")
	}
	return s, nil
}

// parseTime accepts RFC3339 strings (with or without fractional seconds), a
// few common layouts (timestamps without an offset are interpreted in loc),
// and unix timestamps as numbers or numeric strings (seconds, or
// milliseconds when the value is larger than 1e11).
func parseTime(raw json.RawMessage, loc *time.Location) (time.Time, error) {
	var num float64
	if err := json.Unmarshal(raw, &num); err == nil {
		if t := unixTime(num); plausible(t) {
			return t, nil
		}
		return time.Time{}, fmt.Errorf("unix timestamp %v is out of range", num)
	}
	var s string
	if err := json.Unmarshal(raw, &s); err != nil {
		return time.Time{}, errors.New("must be an RFC3339 string or a unix timestamp")
	}
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, errors.New("is empty")
	}
	if n, err := strconv.ParseFloat(s, 64); err == nil {
		if t := unixTime(n); plausible(t) {
			return t, nil
		}
		return time.Time{}, fmt.Errorf("unix timestamp %q is out of range", s)
	}
	layouts := []string{
		time.RFC3339Nano,
		time.RFC3339,
		"2006-01-02T15:04:05.999999999",
		"2006-01-02T15:04:05",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04",
		"2006-01-02 15:04",
	}
	for _, l := range layouts {
		if t, err := time.ParseInLocation(l, s, loc); err == nil {
			if !plausible(t) {
				return time.Time{}, fmt.Errorf("time %q is out of range (2000-2200)", s)
			}
			return t, nil
		}
	}
	return time.Time{}, fmt.Errorf("unrecognized time %q (use RFC3339, e.g. 2026-01-31T18:00:00+01:00)", s)
}

// unixTime converts a unix timestamp (seconds, or milliseconds when > 1e11)
// into a time. Non-finite or absurd values yield the zero time.
func unixTime(n float64) time.Time {
	if math.IsNaN(n) || math.IsInf(n, 0) || n < 0 || n > 1e15 {
		return time.Time{}
	}
	if n > 1e11 { // milliseconds (1e11 s would be the year 5138)
		return time.UnixMilli(int64(n)).UTC()
	}
	sec := int64(n)
	nsec := int64((n - float64(sec)) * 1e9)
	return time.Unix(sec, nsec).UTC()
}

// plausible rejects timestamps outside 2000-01-01 .. 2200-01-01.
func plausible(t time.Time) bool {
	return !t.IsZero() && t.Year() >= 2000 && t.Year() < 2200
}

// SafeID converts an item id into a string that is safe to use as a file or
// directory name. Ids that needed changes (or are long) get a short hash
// suffix so distinct ids can never collide ("a/b" vs "a_b").
func SafeID(id string) string {
	var b strings.Builder
	for _, r := range id {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	s := strings.Trim(b.String(), ".")
	if s == id && len(s) <= 64 && s != "" {
		return s
	}
	if len(s) > 48 {
		s = s[:48]
	}
	s = strings.Trim(s, "._-")
	sum := sha256.Sum256([]byte(id))
	h := hex.EncodeToString(sum[:])[:8]
	if s == "" {
		return "item-" + h
	}
	return s + "-" + h
}

// Fetcher downloads the schedule over HTTP and maintains an on-disk cache.
type Fetcher struct {
	URL        string
	Client     *http.Client
	AuthHeader string // optional "Name: value"
	UserAgent  string
	CachePath  string
	Location   *time.Location

	mu   sync.Mutex
	etag string
}

// NewFetcher creates a Fetcher; cacheDir may be empty to disable caching.
func NewFetcher(rawURL string, timeout time.Duration, authHeader, userAgent, cacheDir string, loc *time.Location) *Fetcher {
	f := &Fetcher{
		URL:        rawURL,
		Client:     &http.Client{Timeout: timeout},
		AuthHeader: authHeader,
		UserAgent:  userAgent,
		Location:   loc,
	}
	if cacheDir != "" {
		f.CachePath = filepath.Join(cacheDir, "schedule.cache.json")
	}
	return f
}

// ErrNotModified is returned by Fetch when the server answered 304.
var ErrNotModified = errors.New("schedule not modified")

// Fetch downloads and parses the schedule. On success the result is also
// written to the cache. It returns ErrNotModified when the server reports the
// document is unchanged since the last successful fetch.
func (f *Fetcher) Fetch(ctx context.Context) (*Schedule, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, f.URL, nil)
	if err != nil {
		return nil, fmt.Errorf("build request: %w", err)
	}
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Cache-Control", "no-cache")
	if f.UserAgent != "" {
		req.Header.Set("User-Agent", f.UserAgent)
	}
	if f.AuthHeader != "" {
		if name, value, ok := strings.Cut(f.AuthHeader, ":"); ok {
			req.Header.Set(strings.TrimSpace(name), strings.TrimSpace(value))
		}
	}
	f.mu.Lock()
	etag := f.etag
	f.mu.Unlock()
	if etag != "" {
		req.Header.Set("If-None-Match", etag)
	}

	resp, err := f.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotModified && etag != "" {
		return nil, ErrNotModified
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return nil, fmt.Errorf("unexpected HTTP status %s", resp.Status)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, MaxDocumentSize+1))
	if err != nil {
		return nil, fmt.Errorf("read body: %w", err)
	}
	if len(body) > MaxDocumentSize {
		return nil, fmt.Errorf("document larger than %d bytes", MaxDocumentSize)
	}
	s, err := Parse(body, f.Location)
	if err != nil {
		return nil, err
	}
	s.FetchedAt = time.Now()
	s.ETag = resp.Header.Get("ETag")
	s.Origin = "url"
	if d := resp.Header.Get("Date"); d != "" {
		if t, err := http.ParseTime(d); err == nil {
			s.ServerDate = t
		}
	}

	f.mu.Lock()
	f.etag = s.ETag
	f.mu.Unlock()

	if f.CachePath != "" {
		if err := f.saveCache(s); err != nil {
			// Caching is best effort; the caller keeps working with the parsed schedule.
			return s, &CacheError{Err: err}
		}
	}
	return s, nil
}

// CacheError wraps a failure to persist the schedule cache. The schedule
// returned alongside it is still valid.
type CacheError struct{ Err error }

func (e *CacheError) Error() string { return "write schedule cache: " + e.Err.Error() }
func (e *CacheError) Unwrap() error { return e.Err }

// cacheDoc is the on-disk format of the schedule cache. Rejected ids are kept
// so that, after a restart, a present-but-invalid item is not mistaken for a
// removed one.
type cacheDoc struct {
	Items     *[]json.RawMessage `json:"items"`
	Invalid   []InvalidItem      `json:"invalid,omitempty"`
	FetchedAt time.Time          `json:"fetchedAt"`
	URL       string             `json:"url"`
}

func (f *Fetcher) saveCache(s *Schedule) error {
	data, err := json.MarshalIndent(struct {
		Items     []Item        `json:"items"`
		Invalid   []InvalidItem `json:"invalid,omitempty"`
		FetchedAt time.Time     `json:"fetchedAt"`
		URL       string        `json:"url"`
	}{s.Items, s.Invalid, s.FetchedAt, f.URL}, "", "  ")
	if err != nil {
		return err
	}
	return WriteFileAtomic(f.CachePath, data, 0o644)
}

// LoadCache reads the last successfully fetched schedule from disk.
func (f *Fetcher) LoadCache() (*Schedule, error) {
	if f.CachePath == "" {
		return nil, errors.New("cache disabled")
	}
	data, err := os.ReadFile(f.CachePath)
	if err != nil {
		return nil, err
	}
	var doc cacheDoc
	if err := json.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("parse cache: %w", err)
	}
	if doc.Items == nil {
		return nil, errors.New("parse cache: missing items array")
	}
	if doc.URL != "" && f.URL != "" && doc.URL != f.URL {
		return nil, errors.New("cache was written for a different SCHEDULE_URL; ignoring it")
	}
	if len(*doc.Items) == 0 {
		return &Schedule{Items: []Item{}, Invalid: doc.Invalid, FetchedAt: doc.FetchedAt, Origin: "cache"}, nil
	}
	s, err := Parse(data, time.UTC) // cache holds RFC3339 with offsets
	if err != nil {
		return nil, fmt.Errorf("parse cache: %w", err)
	}
	s.Invalid = append(s.Invalid, doc.Invalid...)
	s.FetchedAt = doc.FetchedAt
	s.Origin = "cache"
	return s, nil
}

// WriteFileAtomic writes data to a temporary file in the same directory,
// fsyncs it, renames it over the destination and fsyncs the directory.
func WriteFileAtomic(path string, data []byte, perm os.FileMode) error {
	return writeFileAtomic(path, data, perm, true)
}

// WriteFileAtomicNoSync is WriteFileAtomic without fsync: the rename is still
// atomic, durability is left to the OS. Used for frequent progress updates.
func WriteFileAtomicNoSync(path string, data []byte, perm os.FileMode) error {
	return writeFileAtomic(path, data, perm, false)
}

func writeFileAtomic(path string, data []byte, perm os.FileMode, sync bool) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		cleanup()
		return err
	}
	if sync {
		if err := tmp.Sync(); err != nil {
			tmp.Close()
			cleanup()
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		return err
	}
	if err := os.Chmod(tmpName, perm); err != nil {
		cleanup()
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		cleanup()
		return err
	}
	if sync {
		if d, err := os.Open(dir); err == nil {
			_ = d.Sync()
			_ = d.Close()
		}
	}
	return nil
}
