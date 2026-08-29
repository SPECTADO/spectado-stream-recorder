package schedule

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	testSource = "https://stream.example.com/live.aac"
	fSource    = `"source":"https://stream.example.com/live.aac"`
	fStart     = `"start":"2026-01-31T18:00:00Z"`
	fEnd       = `"end":"2026-01-31T19:00:00Z"`
	farEnd     = `"end":"2030-01-01T00:00:00Z"`
)

// obj joins JSON fields into an object literal.
func obj(fields ...string) string { return "{" + strings.Join(fields, ",") + "}" }

// arr joins JSON literals into an array literal.
func arr(items ...string) string { return "[" + strings.Join(items, ",") + "]" }

// valid returns a minimal valid item with the given id plus extra fields.
func valid(id string, extra ...string) string {
	fields := []string{fmt.Sprintf(`"id":%q`, id), fSource, fStart, fEnd}
	return obj(append(fields, extra...)...)
}

func mustParse(t *testing.T, doc string, loc *time.Location) *Schedule {
	t.Helper()
	s, err := Parse([]byte(doc), loc)
	if err != nil {
		t.Fatalf("Parse(%s) unexpected error: %v", doc, err)
	}
	return s
}

func ids(s *Schedule) []string {
	out := make([]string, 0, len(s.Items))
	for _, it := range s.Items {
		out = append(out, it.ID)
	}
	return out
}

func equalStrings(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestParse_DocumentForms(t *testing.T) {
	item := valid("radio-1")
	cases := []struct {
		name string
		doc  string
	}{
		{"top-level array", arr(item)},
		{"items object", `{"items":` + arr(item) + `}`},
		{"streams object", `{"streams":` + arr(item) + `}`},
		{"recordings object", `{"recordings":` + arr(item) + `}`},
		{"null items falls back to streams", `{"items":null,"streams":` + arr(item) + `}`},
		{"surrounding whitespace", "\n\t " + arr(item) + " \n"},
		{"extra top-level fields ignored", `{"version":3,"generatedAt":"x","items":` + arr(item) + `}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := mustParse(t, tc.doc, time.UTC)
			if len(s.Items) != 1 || s.Items[0].ID != "radio-1" {
				t.Fatalf("items = %+v, want one item radio-1", s.Items)
			}
			if len(s.Invalid) != 0 {
				t.Fatalf("Invalid = %+v, want none", s.Invalid)
			}
			it := s.Items[0]
			if it.Source != testSource {
				t.Errorf("Source = %q", it.Source)
			}
			if want := time.Date(2026, 1, 31, 18, 0, 0, 0, time.UTC); !it.Start.Equal(want) {
				t.Errorf("Start = %v, want %v", it.Start, want)
			}
			if want := time.Date(2026, 1, 31, 19, 0, 0, 0, time.UTC); !it.End.Equal(want) {
				t.Errorf("End = %v, want %v", it.End, want)
			}
		})
	}
}

func TestParse_DocumentErrors(t *testing.T) {
	cases := []struct {
		name    string
		doc     string
		wantErr string
	}{
		{"empty input", "", "empty document"},
		{"whitespace only", " \n\t ", "empty document"},
		{"empty object", `{}`, `document must be an array or an object with an "items" array`},
		{"null items", `{"items":null}`, `document must be an array`},
		{"unknown key", `{"foo":[]}`, `document must be an array`},
		{"items not an array", `{"items":5}`, `field "items" is not an array`},
		{"items is an object", `{"items":{"id":"x"}}`, `field "items" is not an array`},
		{"malformed array", `[{"id":"x"`, "invalid JSON array"},
		{"malformed object", `{"items": [`, "invalid JSON document"},
		{"scalar", `42`, "invalid JSON document"},
		{"string", `"hello"`, "invalid JSON document"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s, err := Parse([]byte(tc.doc), time.UTC)
			if err == nil {
				t.Fatalf("Parse(%q) = %+v, want error", tc.doc, s)
			}
			if !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("error = %q, want it to contain %q", err, tc.wantErr)
			}
		})
	}
}

func TestParse_EmptySchedule(t *testing.T) {
	for _, doc := range []string{`[]`, `{"items":[]}`, `{"streams":[]}`, ` [ ] `} {
		t.Run(doc, func(t *testing.T) {
			s := mustParse(t, doc, time.UTC)
			if s.Items == nil {
				t.Fatal("Items is nil, want empty non-nil slice")
			}
			if len(s.Items) != 0 || len(s.Invalid) != 0 {
				t.Fatalf("Items=%v Invalid=%v, want both empty", s.Items, s.Invalid)
			}
		})
	}
}

func TestParse_Aliases(t *testing.T) {
	cases := []struct {
		name string
		item string
		want Item
	}{
		{
			name: "url/startTime/endTime/title/objectKey",
			item: obj(`"id":"a"`, `"url":"`+testSource+`"`, `"startTime":"2026-01-31T18:00:00Z"`,
				`"endTime":"2026-01-31T19:00:00Z"`, `"title":"Radio One"`, `"objectKey":"shows/a.aac"`),
			want: Item{ID: "a", Name: "Radio One", Source: testSource, Key: "shows/a.aac"},
		},
		{
			name: "stream/start_time/end_time/object_key",
			item: obj(`"id":"a"`, `"stream":"`+testSource+`"`, `"start_time":"2026-01-31T18:00:00Z"`,
				`"end_time":"2026-01-31T19:00:00Z"`, `"object_key":"shows/a.aac"`),
			want: Item{ID: "a", Source: testSource, Key: "shows/a.aac"},
		},
		{
			name: "from/to",
			item: obj(`"id":"a"`, fSource, `"from":"2026-01-31T18:00:00Z"`, `"to":"2026-01-31T19:00:00Z"`),
			want: Item{ID: "a", Source: testSource},
		},
		{
			name: "canonical names win over aliases",
			item: obj(`"id":"a"`, fSource, `"url":"https://other.example.com/x"`, fStart, fEnd,
				`"name":"Canonical"`, `"title":"Alias"`, `"key":"k1"`, `"objectKey":"k2"`),
			want: Item{ID: "a", Name: "Canonical", Source: testSource, Key: "k1"},
		},
		{
			name: "null canonical falls back to alias",
			item: obj(`"id":"a"`, `"source":null`, `"url":"`+testSource+`"`, fStart, fEnd, `"name":null`, `"title":"T"`),
			want: Item{ID: "a", Name: "T", Source: testSource},
		},
	}
	start := time.Date(2026, 1, 31, 18, 0, 0, 0, time.UTC)
	end := start.Add(time.Hour)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := mustParse(t, arr(tc.item), time.UTC)
			if len(s.Items) != 1 {
				t.Fatalf("Items = %+v (invalid: %+v), want 1", s.Items, s.Invalid)
			}
			got := s.Items[0]
			if got.ID != tc.want.ID || got.Name != tc.want.Name || got.Source != tc.want.Source || got.Key != tc.want.Key {
				t.Errorf("got %+v, want ID/Name/Source/Key of %+v", got, tc.want)
			}
			if !got.Start.Equal(start) || !got.End.Equal(end) {
				t.Errorf("Start/End = %v/%v, want %v/%v", got.Start, got.End, start, end)
			}
		})
	}
}

func TestParse_TimeFormats(t *testing.T) {
	base := time.Date(2026, 1, 31, 18, 0, 0, 0, time.UTC)
	prague, pragueErr := time.LoadLocation("Europe/Prague")
	cases := []struct {
		name string
		raw  string // JSON literal used as "start"
		loc  *time.Location
		skip bool
		want time.Time
	}{
		{"rfc3339 utc", `"2026-01-31T18:00:00Z"`, time.UTC, false, base},
		{"rfc3339 positive offset", `"2026-01-31T19:00:00+01:00"`, time.UTC, false, base},
		{"rfc3339 negative offset", `"2026-01-31T13:00:00-05:00"`, time.UTC, false, base},
		{"rfc3339 offset ignores loc", `"2026-01-31T19:00:00+01:00"`, prague, pragueErr != nil, base},
		{"rfc3339nano", `"2026-01-31T18:00:00.123456789Z"`, time.UTC, false, base.Add(123456789)},
		{"rfc3339 fractional with offset", `"2026-01-31T19:00:00.5+01:00"`, time.UTC, false, base.Add(500 * time.Millisecond)},
		{"rfc3339 padded", `"  2026-01-31T18:00:00Z  "`, time.UTC, false, base},
		{"naive in UTC", `"2026-01-31T18:00:00"`, time.UTC, false, base},
		{"naive with nil loc", `"2026-01-31T18:00:00"`, nil, false, base},
		{"naive in Prague (winter, +01:00)", `"2026-01-31T19:00:00"`, prague, pragueErr != nil, base},
		{"naive in Prague (summer, +02:00)", `"2026-07-15T20:00:00"`, prague, pragueErr != nil, time.Date(2026, 7, 15, 18, 0, 0, 0, time.UTC)},
		{"naive fractional", `"2026-01-31T18:00:00.25"`, time.UTC, false, base.Add(250 * time.Millisecond)},
		{"naive space separator", `"2026-01-31 18:00:00"`, time.UTC, false, base},
		{"naive minutes only", `"2026-01-31T18:00"`, time.UTC, false, base},
		{"naive space minutes only", `"2026-01-31 18:00"`, time.UTC, false, base},
		{"unix seconds number", fmt.Sprintf("%d", base.Unix()), time.UTC, false, base},
		{"unix seconds fractional number", fmt.Sprintf("%d.5", base.Unix()), time.UTC, false, base.Add(500 * time.Millisecond)},
		{"unix seconds numeric string", fmt.Sprintf(`"%d"`, base.Unix()), time.UTC, false, base},
		{"unix seconds padded numeric string", fmt.Sprintf(`" %d "`, base.Unix()), time.UTC, false, base},
		{"unix milliseconds number", fmt.Sprintf("%d", base.UnixMilli()), time.UTC, false, base},
		{"unix milliseconds numeric string", fmt.Sprintf(`"%d"`, base.UnixMilli()), time.UTC, false, base},
		{"unix seconds ignore loc", fmt.Sprintf("%d", base.Unix()), prague, pragueErr != nil, base},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skipf("time zone unavailable: %v", pragueErr)
			}
			doc := arr(obj(`"id":"a"`, fSource, `"start":`+tc.raw, farEnd))
			s := mustParse(t, doc, tc.loc)
			if len(s.Items) != 1 {
				t.Fatalf("Items = %+v (invalid: %+v), want 1", s.Items, s.Invalid)
			}
			if got := s.Items[0].Start; !got.Equal(tc.want) {
				t.Errorf("Start = %v (unix %d), want %v (unix %d)", got, got.UnixNano(), tc.want, tc.want.UnixNano())
			}
		})
	}
}

func TestParse_TimeErrors(t *testing.T) {
	cases := []struct {
		name    string
		start   string
		end     string
		wantErr string
	}{
		{"unrecognized layout", `"31.01.2026 18:00"`, farEnd, "start: unrecognized time"},
		{"bool", `true`, farEnd, "start: must be an RFC3339 string or a unix timestamp"},
		{"object", `{"y":2026}`, farEnd, "start: must be an RFC3339 string or a unix timestamp"},
		{"empty string", `""`, farEnd, "start: is empty"},
		{"blank string", `"   "`, farEnd, "start: is empty"},
		{"end garbage", fStart[8:], `"end":"tomorrow"`, "end: unrecognized time"},
		{"end null counts as missing", fStart[8:], `"end":null`, "end is required"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			doc := arr(valid("ok"), obj(`"id":"bad"`, fSource, `"start":`+tc.start, tc.end))
			s := mustParse(t, doc, time.UTC)
			if len(s.Items) != 1 || s.Items[0].ID != "ok" {
				t.Fatalf("Items = %+v, want only ok", s.Items)
			}
			if len(s.Invalid) != 1 {
				t.Fatalf("Invalid = %+v, want 1", s.Invalid)
			}
			inv := s.Invalid[0]
			if inv.Index != 1 || inv.ID != "bad" {
				t.Errorf("Invalid = %+v, want index 1 id bad", inv)
			}
			if !strings.Contains(inv.Reason, tc.wantErr) {
				t.Errorf("reason = %q, want it to contain %q", inv.Reason, tc.wantErr)
			}
		})
	}
}

func TestParse_IDForms(t *testing.T) {
	cases := []struct {
		name   string
		id     string // JSON literal
		wantID string
	}{
		{"string", `"radio-1"`, "radio-1"},
		{"integer", `42`, "42"},
		{"large integer keeps digits", `12345678901234567`, "12345678901234567"},
		{"float", `4.5`, "4.5"},
		{"padded string is trimmed", `"  spaced  "`, "spaced"},
		{"unicode", `"Český rozhlas"`, "Český rozhlas"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := mustParse(t, arr(obj(`"id":`+tc.id, fSource, fStart, fEnd)), time.UTC)
			if len(s.Items) != 1 {
				t.Fatalf("Items = %+v (invalid: %+v), want 1", s.Items, s.Invalid)
			}
			if s.Items[0].ID != tc.wantID {
				t.Errorf("ID = %q, want %q", s.Items[0].ID, tc.wantID)
			}
		})
	}
}

func TestParse_InvalidItems(t *testing.T) {
	longID := strings.Repeat("i", 129)
	longKey := strings.Repeat("k", 901)
	cases := []struct {
		name    string
		item    string
		wantID  string
		wantErr string
	}{
		{"not an object", `"just a string"`, "", "item is not an object"},
		{"not an object (number)", `7`, "", "item is not an object"},
		{"id missing", obj(fSource, fStart, fEnd), "", "id is required"},
		{"id null", obj(`"id":null`, fSource, fStart, fEnd), "", "id is required"},
		{"id blank", obj(`"id":"   "`, fSource, fStart, fEnd), "", "id is required"},
		{"id bool", obj(`"id":true`, fSource, fStart, fEnd), "", "id must be a string or number"},
		{"id object", obj(`"id":{"x":1}`, fSource, fStart, fEnd), "", "id must be a string or number"},
		{"id too long", obj(`"id":"`+longID+`"`, fSource, fStart, fEnd), longID, "id is longer than 128 characters"},
		{"name not a string", obj(`"id":"x"`, `"name":1`, fSource, fStart, fEnd), "x", "name: must be a string"},
		{"source missing", obj(`"id":"x"`, fStart, fEnd), "x", "source is required"},
		{"source blank", obj(`"id":"x"`, `"source":"  "`, fStart, fEnd), "x", "source is required"},
		{"source not a string", obj(`"id":"x"`, `"source":5`, fStart, fEnd), "x", "source: must be a string"},
		{"source ftp", obj(`"id":"x"`, `"source":"ftp://example.com/a"`, fStart, fEnd), "x", "source must be an http(s) URL"},
		{"source rtmp", obj(`"id":"x"`, `"source":"rtmp://example.com/a"`, fStart, fEnd), "x", "source must be an http(s) URL"},
		{"source no host", obj(`"id":"x"`, `"source":"http:///path"`, fStart, fEnd), "x", "source must be an http(s) URL"},
		{"source relative", obj(`"id":"x"`, `"source":"live.aac"`, fStart, fEnd), "x", "source must be an http(s) URL"},
		{"source garbage", obj(`"id":"x"`, `"source":"::not a url::"`, fStart, fEnd), "x", "source must be an http(s) URL"},
		{"type invalid", obj(`"id":"x"`, fSource, fStart, fEnd, `"type":"rtmp"`), "x", `type must be hls, icecast or auto (got "rtmp")`},
		{"type not a string", obj(`"id":"x"`, fSource, fStart, fEnd, `"type":1`), "x", "type: must be a string"},
		{"start missing", obj(`"id":"x"`, fSource, fEnd), "x", "start is required"},
		{"end missing", obj(`"id":"x"`, fSource, fStart), "x", "end is required"},
		{"end equals start", obj(`"id":"x"`, fSource, fStart, `"end":"2026-01-31T18:00:00Z"`), "x", "end must be after start"},
		{"end before start", obj(`"id":"x"`, fSource, fStart, `"end":"2026-01-31T17:00:00Z"`), "x", "end must be after start"},
		{"end equals start across offsets", obj(`"id":"x"`, fSource, fStart, `"end":"2026-01-31T19:00:00+01:00"`), "x", "end must be after start"},
		{"key with dotdot", obj(`"id":"x"`, fSource, fStart, fEnd, `"key":"a/../b.aac"`), "x", "key must not contain '..' or line breaks"},
		{"key with newline", obj(`"id":"x"`, fSource, fStart, fEnd, `"key":"a\nb"`), "x", "key must not contain '..' or line breaks"},
		{"key with carriage return", obj(`"id":"x"`, fSource, fStart, fEnd, `"key":"a\rb"`), "x", "key must not contain '..' or line breaks"},
		{"key too long", obj(`"id":"x"`, fSource, fStart, fEnd, `"key":"`+longKey+`"`), "x", "key is longer than 900 bytes"},
		{"key not a string", obj(`"id":"x"`, fSource, fStart, fEnd, `"key":["a"]`), "x", "key: must be a string"},
		{"codec invalid", obj(`"id":"x"`, fSource, fStart, fEnd, `"codec":"mp3"`), "x", `codec must be auto, aac or copy (got "mp3")`},
		{"codec not a string", obj(`"id":"x"`, fSource, fStart, fEnd, `"codec":true`), "x", "codec: must be a string"},
		{"bitrate invalid string", obj(`"id":"x"`, fSource, fStart, fEnd, `"bitrate":"abc"`), "x", `bitrate "abc" is not a valid ffmpeg bitrate`},
		{"bitrate zero", obj(`"id":"x"`, fSource, fStart, fEnd, `"bitrate":0`), "x", "bitrate must be a string like"},
		{"bitrate negative", obj(`"id":"x"`, fSource, fStart, fEnd, `"bitrate":-128`), "x", "bitrate must be a string like"},
		{"bitrate bool", obj(`"id":"x"`, fSource, fStart, fEnd, `"bitrate":true`), "x", "bitrate must be a string like"},
		{"bitrate negative string", obj(`"id":"x"`, fSource, fStart, fEnd, `"bitrate":"-5k"`), "x", "is not a valid ffmpeg bitrate"},
		{"headers not an object", obj(`"id":"x"`, fSource, fStart, fEnd, `"headers":"X: y"`), "x", "headers must be an object of strings"},
		{"headers non-string value", obj(`"id":"x"`, fSource, fStart, fEnd, `"headers":{"X-A":1}`), "x", "headers must be an object of strings"},
		{"header value with CRLF", obj(`"id":"x"`, fSource, fStart, fEnd, `"headers":{"X-A":"a\r\nInjected: b"}`), "x", `headers: invalid header "X-A"`},
		{"header value with LF", obj(`"id":"x"`, fSource, fStart, fEnd, `"headers":{"X-A":"a\nb"}`), "x", `headers: invalid header "X-A"`},
		{"header name with colon", obj(`"id":"x"`, fSource, fStart, fEnd, `"headers":{"X-A:":"b"}`), "x", "headers: invalid header"},
		{"header name with CR", obj(`"id":"x"`, fSource, fStart, fEnd, `"headers":{"X\r-A":"b"}`), "x", "headers: invalid header"},
		{"header name blank", obj(`"id":"x"`, fSource, fStart, fEnd, `"headers":{"  ":"b"}`), "x", "headers: invalid header"},
		{"startEarly too large", obj(`"id":"x"`, fSource, fStart, fEnd, `"startEarly":"7h"`), "x", "startEarly must be between 0 and 6h"},
		{"startEarly negative", obj(`"id":"x"`, fSource, fStart, fEnd, `"startEarly":"-1s"`), "x", "startEarly must be between 0 and 6h"},
		{"startEarly negative number", obj(`"id":"x"`, fSource, fStart, fEnd, `"startEarly":-5`), "x", "startEarly must be between 0 and 6h"},
		{"startEarly invalid", obj(`"id":"x"`, fSource, fStart, fEnd, `"startEarly":"soon"`), "x", `startEarly: invalid duration "soon"`},
		{"startEarly bool", obj(`"id":"x"`, fSource, fStart, fEnd, `"startEarly":true`), "x", "startEarly: must be a duration string or a number of seconds"},
		{"stopLate too large (number seconds)", obj(`"id":"x"`, fSource, fStart, fEnd, `"stopLate":21601`), "x", "stopLate must be between 0 and 6h"},
		{"stop_late too large", obj(`"id":"x"`, fSource, fStart, fEnd, `"stop_late":"6h1s"`), "x", "stopLate must be between 0 and 6h"},
		{"stallTimeout too small", obj(`"id":"x"`, fSource, fStart, fEnd, `"stallTimeout":"4s"`), "x", "stallTimeout must be at least 5s"},
		{"stallTimeout zero", obj(`"id":"x"`, fSource, fStart, fEnd, `"stallTimeout":0`), "x", "stallTimeout must be at least 5s"},
		{"stallTimeout negative", obj(`"id":"x"`, fSource, fStart, fEnd, `"stallTimeout":"-5s"`), "x", "stallTimeout must be between 0 and 6h"},
		{"stall_timeout too large", obj(`"id":"x"`, fSource, fStart, fEnd, `"stall_timeout":"7h"`), "x", "stallTimeout must be between 0 and 6h"},
		{"stallTimeout invalid", obj(`"id":"x"`, fSource, fStart, fEnd, `"stallTimeout":"later"`), "x", `stallTimeout: invalid duration "later"`},
		{"insecureTLS string", obj(`"id":"x"`, fSource, fStart, fEnd, `"insecureTLS":"yes"`), "x", "insecureTLS must be a boolean"},
		{"insecureTLS number", obj(`"id":"x"`, fSource, fStart, fEnd, `"insecure_tls":1`), "x", "insecureTLS must be a boolean"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := mustParse(t, arr(valid("ok"), tc.item), time.UTC)
			if got := ids(s); !equalStrings(got, []string{"ok"}) {
				t.Fatalf("valid ids = %v, want [ok]; invalid = %+v", got, s.Invalid)
			}
			if len(s.Invalid) != 1 {
				t.Fatalf("Invalid = %+v, want exactly 1", s.Invalid)
			}
			inv := s.Invalid[0]
			if inv.Index != 1 {
				t.Errorf("Index = %d, want 1", inv.Index)
			}
			if inv.ID != tc.wantID {
				t.Errorf("ID = %q, want %q", inv.ID, tc.wantID)
			}
			if !strings.Contains(inv.Reason, tc.wantErr) {
				t.Errorf("Reason = %q, want it to contain %q", inv.Reason, tc.wantErr)
			}
		})
	}
}

func TestParse_Duplicates(t *testing.T) {
	t.Run("duplicate id rejects the second", func(t *testing.T) {
		doc := arr(valid("radio-1", `"name":"first"`), valid("radio-1", `"name":"second"`), valid("radio-2"))
		s := mustParse(t, doc, time.UTC)
		if got := ids(s); !equalStrings(got, []string{"radio-1", "radio-2"}) {
			t.Fatalf("ids = %v", got)
		}
		if it, _ := s.Get("radio-1"); it.Name != "first" {
			t.Errorf("kept item = %+v, want the first occurrence", it)
		}
		if len(s.Invalid) != 1 || s.Invalid[0].Index != 1 || s.Invalid[0].ID != "radio-1" || s.Invalid[0].Reason != "duplicate id" {
			t.Errorf("Invalid = %+v", s.Invalid)
		}
	})
	t.Run("duplicate id after trimming", func(t *testing.T) {
		s := mustParse(t, arr(valid("radio-1"), valid("  radio-1 ")), time.UTC)
		if len(s.Items) != 1 || len(s.Invalid) != 1 || s.Invalid[0].Reason != "duplicate id" {
			t.Fatalf("Items=%+v Invalid=%+v", s.Items, s.Invalid)
		}
	})
	t.Run("duplicate explicit key rejects the second", func(t *testing.T) {
		doc := arr(valid("radio-1", `"key":"shows/a.aac"`), valid("radio-2", `"key":"/shows/a.aac"`), valid("radio-3", `"key":"shows/b.aac"`))
		s := mustParse(t, doc, time.UTC)
		if got := ids(s); !equalStrings(got, []string{"radio-1", "radio-3"}) {
			t.Fatalf("ids = %v; invalid = %+v", got, s.Invalid)
		}
		if len(s.Invalid) != 1 {
			t.Fatalf("Invalid = %+v", s.Invalid)
		}
		inv := s.Invalid[0]
		if inv.Index != 1 || inv.ID != "radio-2" {
			t.Errorf("Invalid = %+v, want index 1 id radio-2", inv)
		}
		if want := `key "shows/a.aac" already used by item "radio-1"`; inv.Reason != want {
			t.Errorf("Reason = %q, want %q", inv.Reason, want)
		}
	})
	t.Run("items without keys never collide", func(t *testing.T) {
		s := mustParse(t, arr(valid("a"), valid("b"), valid("c", `"key":""`)), time.UTC)
		if len(s.Items) != 3 || len(s.Invalid) != 0 {
			t.Fatalf("Items=%v Invalid=%+v", ids(s), s.Invalid)
		}
	})
	t.Run("rejected item does not reserve its key", func(t *testing.T) {
		// The first item is invalid (bad codec), so the second may use the same key.
		doc := arr(valid("a", `"key":"k"`, `"codec":"mp3"`), valid("b", `"key":"k"`))
		s := mustParse(t, doc, time.UTC)
		if got := ids(s); !equalStrings(got, []string{"b"}) {
			t.Fatalf("ids = %v; invalid = %+v", got, s.Invalid)
		}
	})
}

func TestParse_AllInvalidIsError(t *testing.T) {
	doc := arr(obj(fSource, fStart, fEnd), valid("x", `"codec":"mp3"`))
	s, err := Parse([]byte(doc), time.UTC)
	if err == nil {
		t.Fatalf("Parse = %+v, want error", s)
	}
	if !strings.Contains(err.Error(), "all 2 items are invalid") || !strings.Contains(err.Error(), "id is required") {
		t.Errorf("error = %q", err)
	}

	// A single invalid item is also "all invalid".
	if _, err := Parse([]byte(arr(valid("x", `"type":"rtmp"`))), time.UTC); err == nil || !strings.Contains(err.Error(), "all 1 items are invalid") {
		t.Errorf("single invalid item: err = %v", err)
	}

	// Duplicates alone never make a document all-invalid: the first wins.
	if s, err := Parse([]byte(arr(valid("x"), valid("x"))), time.UTC); err != nil || len(s.Items) != 1 {
		t.Errorf("duplicates: s=%+v err=%v", s, err)
	}
}

func TestParse_SortedByStart(t *testing.T) {
	doc := arr(
		obj(`"id":"late"`, fSource, `"start":"2026-01-31T20:00:00Z"`, farEnd),
		obj(`"id":"early"`, fSource, `"start":"2026-01-31T18:00:00Z"`, farEnd),
		obj(`"id":"mid-b"`, fSource, `"start":"2026-01-31T19:00:00Z"`, farEnd),
		obj(`"id":"mid-a"`, fSource, `"start":"2026-01-31T20:00:00+01:00"`, farEnd), // same instant as mid-b
		obj(`"id":"broken"`, fSource, `"start":"2026-01-31T00:00:00Z"`, `"end":"2025-01-01T00:00:00Z"`),
	)
	s := mustParse(t, doc, time.UTC)
	// Stable sort: mid-b keeps its position before mid-a (same start instant).
	if got := ids(s); !equalStrings(got, []string{"early", "mid-b", "mid-a", "late"}) {
		t.Errorf("order = %v", got)
	}
	if len(s.Invalid) != 1 || s.Invalid[0].ID != "broken" || s.Invalid[0].Index != 4 {
		t.Errorf("Invalid = %+v", s.Invalid)
	}
}

func TestParse_OptionalFields(t *testing.T) {
	doc := arr(valid("full",
		`"name":"Radio One"`, `"type":" HLS "`, `"codec":"AAC"`, `"bitrate":" 128k "`,
		`"key":"/shows/one.aac"`, `"headers":{"X-Auth":"secret","User-Agent":"ua/1"}`,
		`"startEarly":"15s"`, `"stopLate":45`, `"stallTimeout":"90s"`, `"insecureTLS":true`))
	s := mustParse(t, doc, time.UTC)
	if len(s.Items) != 1 {
		t.Fatalf("Items=%v Invalid=%+v", s.Items, s.Invalid)
	}
	it := s.Items[0]
	if it.Name != "Radio One" {
		t.Errorf("Name = %q", it.Name)
	}
	if it.Type != "hls" {
		t.Errorf("Type = %q, want lower-cased trimmed hls", it.Type)
	}
	if it.Codec != "aac" {
		t.Errorf("Codec = %q, want aac", it.Codec)
	}
	if it.Bitrate != "128k" {
		t.Errorf("Bitrate = %q, want 128k", it.Bitrate)
	}
	if it.Key != "shows/one.aac" {
		t.Errorf("Key = %q, want leading slash removed", it.Key)
	}
	if it.Headers["X-Auth"] != "secret" || it.Headers["User-Agent"] != "ua/1" || len(it.Headers) != 2 {
		t.Errorf("Headers = %v", it.Headers)
	}
	if it.StartEarly == nil || it.StartEarly.D() != 15*time.Second {
		t.Errorf("StartEarly = %v, want 15s", it.StartEarly)
	}
	if it.StopLate == nil || it.StopLate.D() != 45*time.Second {
		t.Errorf("StopLate = %v, want 45s", it.StopLate)
	}
	if it.StallTimeout == nil || it.StallTimeout.D() != 90*time.Second {
		t.Errorf("StallTimeout = %v, want 90s", it.StallTimeout)
	}
	if !it.InsecureTLS {
		t.Error("InsecureTLS = false, want true")
	}

	// Absent optional fields stay zero / nil.
	s = mustParse(t, arr(valid("bare")), time.UTC)
	it = s.Items[0]
	if it.Name != "" || it.Type != "" || it.Codec != "" || it.Bitrate != "" || it.Key != "" || it.Headers != nil ||
		it.StartEarly != nil || it.StopLate != nil || it.StallTimeout != nil || it.InsecureTLS {
		t.Errorf("bare item has unexpected optional values: %+v", it)
	}

	// stallTimeout accepts the 5s lower bound and snake_case; insecureTLS false is honoured.
	s = mustParse(t, arr(valid("a", `"stall_timeout":5`, `"insecure_tls":false`), valid("b", `"stallTimeout":"6h"`)), time.UTC)
	if len(s.Items) != 2 {
		t.Fatalf("Items=%v Invalid=%+v", ids(s), s.Invalid)
	}
	if a, _ := s.Get("a"); a.StallTimeout == nil || a.StallTimeout.D() != 5*time.Second || a.InsecureTLS {
		t.Errorf("a = %+v", a)
	}
	if b, _ := s.Get("b"); b.StallTimeout == nil || b.StallTimeout.D() != 6*time.Hour {
		t.Errorf("b.StallTimeout = %v", b.StallTimeout)
	}
}

func TestParse_Durations(t *testing.T) {
	cases := []struct {
		name  string
		field string
		want  time.Duration
	}{
		{"string seconds", `"startEarly":"15s"`, 15 * time.Second},
		{"string minutes", `"startEarly":"1m30s"`, 90 * time.Second},
		{"number seconds", `"startEarly":20`, 20 * time.Second},
		{"fractional number seconds", `"startEarly":1.5`, 1500 * time.Millisecond},
		{"numeric string", `"startEarly":"45"`, 45 * time.Second},
		{"zero string", `"startEarly":"0s"`, 0},
		{"zero number", `"startEarly":0`, 0},
		{"snake_case alias", `"start_early":"10s"`, 10 * time.Second},
		{"upper bound", `"startEarly":"6h"`, 6 * time.Hour},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := mustParse(t, arr(valid("x", tc.field)), time.UTC)
			if len(s.Items) != 1 {
				t.Fatalf("Items=%v Invalid=%+v", s.Items, s.Invalid)
			}
			got := s.Items[0].StartEarly
			if got == nil {
				t.Fatal("StartEarly is nil, want a value")
			}
			if got.D() != tc.want {
				t.Errorf("StartEarly = %v, want %v", got.D(), tc.want)
			}
		})
	}
	t.Run("stopLate variants", func(t *testing.T) {
		s := mustParse(t, arr(valid("a", `"stopLate":"30s"`), valid("b", `"stop_late":60`)), time.UTC)
		if len(s.Items) != 2 {
			t.Fatalf("Items=%v Invalid=%+v", s.Items, s.Invalid)
		}
		if a, _ := s.Get("a"); a.StopLate == nil || a.StopLate.D() != 30*time.Second {
			t.Errorf("a.StopLate = %v", a.StopLate)
		}
		if b, _ := s.Get("b"); b.StopLate == nil || b.StopLate.D() != time.Minute {
			t.Errorf("b.StopLate = %v", b.StopLate)
		}
	})
}

func TestParse_Bitrate(t *testing.T) {
	cases := []struct {
		name  string
		field string
		want  string
	}{
		{"string k", `"bitrate":"128k"`, "128k"},
		{"string upper K", `"bitrate":"128K"`, "128K"},
		{"string m", `"bitrate":"1.5M"`, "1.5M"},
		{"string padded", `"bitrate":"  96k "`, "96k"},
		{"plain number string", `"bitrate":"128000"`, "128000"},
		{"number", `"bitrate":128000`, "128000"},
		{"number truncates fraction", `"bitrate":128000.9`, "128000"},
		{"empty string skips validation", `"bitrate":""`, ""},
		{"null is absent", `"bitrate":null`, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			s := mustParse(t, arr(valid("x", tc.field)), time.UTC)
			if len(s.Items) != 1 {
				t.Fatalf("Items=%v Invalid=%+v", s.Items, s.Invalid)
			}
			if got := s.Items[0].Bitrate; got != tc.want {
				t.Errorf("Bitrate = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestSchedule_Lookups(t *testing.T) {
	doc := arr(valid("ok"), valid("dup"), valid("dup"), valid("bad", `"codec":"mp3"`), obj(fSource, fStart, fEnd))
	s := mustParse(t, doc, time.UTC)

	if it, ok := s.Get("ok"); !ok || it.ID != "ok" {
		t.Errorf("Get(ok) = %+v, %v", it, ok)
	}
	if it, ok := s.Get("bad"); ok {
		t.Errorf("Get(bad) = %+v, want miss for an invalid item", it)
	}
	if _, ok := s.Get("absent"); ok {
		t.Error("Get(absent) = ok, want miss")
	}
	if _, ok := s.Get(""); ok {
		t.Error("Get(\"\") = ok, want miss")
	}

	for id, want := range map[string]bool{"ok": true, "dup": true, "bad": true, "absent": false, "": false} {
		if got := s.Present(id); got != want {
			t.Errorf("Present(%q) = %v, want %v", id, got, want)
		}
	}

	if got := s.InvalidReason("bad"); !strings.Contains(got, "codec must be auto, aac or copy") {
		t.Errorf("InvalidReason(bad) = %q", got)
	}
	if got := s.InvalidReason("dup"); got != "duplicate id" {
		t.Errorf("InvalidReason(dup) = %q", got)
	}
	if got := s.InvalidReason("ok"); got != "" {
		t.Errorf("InvalidReason(ok) = %q, want empty", got)
	}
	if got := s.InvalidReason("absent"); got != "" {
		t.Errorf("InvalidReason(absent) = %q, want empty", got)
	}

	var nilSched *Schedule
	if _, ok := nilSched.Get("ok"); ok {
		t.Error("nil.Get = ok")
	}
	if nilSched.Present("ok") {
		t.Error("nil.Present = true")
	}
	if nilSched.InvalidReason("ok") != "" {
		t.Error("nil.InvalidReason != \"\"")
	}
}

var safeIDRe = regexp.MustCompile(`^[A-Za-z0-9._-]+$`)

func TestSafeID(t *testing.T) {
	hashed := regexp.MustCompile(`^(.*)-([0-9a-f]{8})$`)
	cases := []struct {
		name       string
		id         string
		wantExact  string // when non-empty the result must equal this
		wantPrefix string // otherwise: the part before "-<8 hex>"
	}{
		{"plain", "radio-1", "radio-1", ""},
		{"mixed safe chars", "abc_DEF.9", "abc_DEF.9", ""},
		{"underscore already safe", "a_b", "a_b", ""},
		{"exactly 64 safe chars", strings.Repeat("x", 64), strings.Repeat("x", 64), ""},
		{"dash only", "-", "-", ""},
		{"slash", "a/b", "", "a_b"},
		{"colon", "a:b", "", "a_b"},
		{"space", "a b", "", "a_b"},
		{"unicode (replacement underscores trimmed from the ends)", "Český", "", "esk"},
		{"unicode inside", "aČb", "", "a_b"},
		{"parentheses", "Radio One (CZ)", "", "Radio_One__CZ"},
		{"leading dot", ".hidden", "", "hidden"},
		{"trailing dot", "name.", "", "name"},
		{"65 safe chars truncated to 48", strings.Repeat("x", 65), "", strings.Repeat("x", 48)},
		{"200 chars", strings.Repeat("ab", 100), "", strings.Repeat("ab", 24)},
		{"trailing separators trimmed after truncation", strings.Repeat("y", 47) + "-" + strings.Repeat("z", 30), "", strings.Repeat("y", 47)},
		{"empty", "", "", "item"},
		{"dots only", "...", "", "item"},
		{"underscores only are already safe", "___", "___", ""},
		{"spaces only", "   ", "", "item"},
		{"path traversal", "../../etc/passwd", "", "etc_passwd"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := SafeID(tc.id)
			if got != SafeID(tc.id) {
				t.Fatalf("SafeID(%q) is not deterministic: %q vs %q", tc.id, got, SafeID(tc.id))
			}
			if got == "" || len(got) > 64 {
				t.Fatalf("SafeID(%q) = %q, want 1..64 chars", tc.id, got)
			}
			if !safeIDRe.MatchString(got) {
				t.Fatalf("SafeID(%q) = %q contains unsafe characters", tc.id, got)
			}
			if tc.wantExact != "" {
				if got != tc.wantExact {
					t.Fatalf("SafeID(%q) = %q, want %q", tc.id, got, tc.wantExact)
				}
				return
			}
			m := hashed.FindStringSubmatch(got)
			if m == nil {
				t.Fatalf("SafeID(%q) = %q, want <prefix>-<8 hex>", tc.id, got)
			}
			if m[1] != tc.wantPrefix {
				t.Errorf("SafeID(%q) = %q, want prefix %q", tc.id, got, tc.wantPrefix)
			}
		})
	}

	t.Run("colliding sanitized bases get different hashes", func(t *testing.T) {
		a, b, c := SafeID("a/b"), SafeID("a:b"), SafeID("a b")
		if a == b || a == c || b == c {
			t.Errorf("collision: %q %q %q", a, b, c)
		}
		for _, v := range []string{a, b, c} {
			if !strings.HasPrefix(v, "a_b-") {
				t.Errorf("%q does not start with a_b-", v)
			}
		}
		if a == SafeID("a_b") {
			t.Errorf("SafeID(a/b) = %q collides with the plain id a_b", a)
		}
	})

	t.Run("properties over nasty ids", func(t *testing.T) {
		nasty := []string{
			"", " ", ".", "..", "/", "a/b", "a:b", "a b", "../../etc/passwd", "ünïcödé", "日本語",
			strings.Repeat("x", 200), strings.Repeat("/", 100), "with\nnewline", "tab\tid", "quote\"id", "null\x00byte",
			"CON", "trailing space ", " leading space", "mixed/Case:ID 1",
		}
		seen := make(map[string]string, len(nasty))
		for _, id := range nasty {
			got := SafeID(id)
			if got == "" || len(got) > 64 {
				t.Errorf("SafeID(%q) = %q, want 1..64 chars", id, got)
			}
			if strings.ContainsAny(got, "/ \t\n\x00") {
				t.Errorf("SafeID(%q) = %q contains a separator or whitespace", id, got)
			}
			if !safeIDRe.MatchString(got) {
				t.Errorf("SafeID(%q) = %q contains unsafe characters", id, got)
			}
			if got != SafeID(id) {
				t.Errorf("SafeID(%q) is not deterministic", id)
			}
			if prev, dup := seen[got]; dup {
				t.Errorf("SafeID(%q) and SafeID(%q) both produce %q", prev, id, got)
			}
			seen[got] = id
		}
	})
}

func TestDuration_JSON(t *testing.T) {
	t.Run("marshal", func(t *testing.T) {
		cases := map[Duration]string{
			Duration(15 * time.Second):        `"15s"`,
			Duration(90 * time.Second):        `"1m30s"`,
			Duration(0):                       `"0s"`,
			Duration(1500 * time.Millisecond): `"1.5s"`,
			Duration(6 * time.Hour):           `"6h0m0s"`,
		}
		for d, want := range cases {
			b, err := json.Marshal(d)
			if err != nil {
				t.Fatalf("Marshal(%v): %v", d, err)
			}
			if string(b) != want {
				t.Errorf("Marshal(%v) = %s, want %s", time.Duration(d), b, want)
			}
		}
	})
	t.Run("unmarshal", func(t *testing.T) {
		cases := []struct {
			raw     string
			want    time.Duration
			wantErr string
		}{
			{`"1m30s"`, 90 * time.Second, ""},
			{`"15s"`, 15 * time.Second, ""},
			{`" 15s "`, 15 * time.Second, ""},
			{`"1h"`, time.Hour, ""},
			{`"250ms"`, 250 * time.Millisecond, ""},
			{`90`, 90 * time.Second, ""},
			{`0`, 0, ""},
			{`1.5`, 1500 * time.Millisecond, ""},
			{`-5`, -5 * time.Second, ""},
			{`"90"`, 90 * time.Second, ""},
			{`"2.5"`, 2500 * time.Millisecond, ""},
			{`"abc"`, 0, `invalid duration "abc"`},
			{`"15 seconds"`, 0, `invalid duration "15 seconds"`},
			{`""`, 0, `invalid duration ""`},
			{`null`, 0, ""}, // encoding/json treats null as a no-op for numbers
			{`true`, 0, "must be a duration string or a number of seconds"},
			{`[1]`, 0, "must be a duration string or a number of seconds"},
			{`{"s":1}`, 0, "must be a duration string or a number of seconds"},
		}
		for _, tc := range cases {
			var d Duration
			err := json.Unmarshal([]byte(tc.raw), &d)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Errorf("Unmarshal(%s) err = %v, want %q", tc.raw, err, tc.wantErr)
				}
				continue
			}
			if err != nil {
				t.Errorf("Unmarshal(%s): %v", tc.raw, err)
				continue
			}
			if d.D() != tc.want {
				t.Errorf("Unmarshal(%s) = %v, want %v", tc.raw, d.D(), tc.want)
			}
		}
	})
	t.Run("round trip inside a struct", func(t *testing.T) {
		type wrapper struct {
			A Duration  `json:"a"`
			B *Duration `json:"b,omitempty"`
			C *Duration `json:"c,omitempty"`
		}
		b := Duration(2 * time.Minute)
		in := wrapper{A: Duration(15 * time.Second), B: &b}
		data, err := json.Marshal(in)
		if err != nil {
			t.Fatal(err)
		}
		if string(data) != `{"a":"15s","b":"2m0s"}` {
			t.Errorf("marshal = %s", data)
		}
		var out wrapper
		if err := json.Unmarshal(data, &out); err != nil {
			t.Fatal(err)
		}
		if out.A != in.A || out.B == nil || *out.B != b || out.C != nil {
			t.Errorf("round trip = %+v, want %+v", out, in)
		}
	})
}

func TestWriteFileAtomic(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "nested", "deeper", "file.json")

	if err := WriteFileAtomic(path, []byte("one"), 0o600); err != nil {
		t.Fatalf("WriteFileAtomic: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if string(got) != "one" {
		t.Errorf("content = %q, want %q", got, "one")
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("perm = %o, want 600", fi.Mode().Perm())
	}
	assertNoTempFiles := func() {
		t.Helper()
		entries, err := os.ReadDir(filepath.Dir(path))
		if err != nil {
			t.Fatal(err)
		}
		if len(entries) != 1 || entries[0].Name() != "file.json" {
			names := make([]string, 0, len(entries))
			for _, e := range entries {
				names = append(names, e.Name())
			}
			t.Errorf("directory contains %v, want only file.json", names)
		}
		for _, e := range entries {
			if strings.Contains(e.Name(), ".tmp-") {
				t.Errorf("leftover temp file %q", e.Name())
			}
		}
	}
	assertNoTempFiles()

	// Overwrite with different content and mode.
	if err := WriteFileAtomic(path, []byte("two, and longer than before"), 0o644); err != nil {
		t.Fatalf("WriteFileAtomic (overwrite): %v", err)
	}
	got, err = os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != "two, and longer than before" {
		t.Errorf("content after overwrite = %q", got)
	}
	if fi, _ := os.Stat(path); fi.Mode().Perm() != 0o644 {
		t.Errorf("perm after overwrite = %o, want 644", fi.Mode().Perm())
	}
	assertNoTempFiles()

	// Overwrite with shorter content must not leave a tail behind.
	if err := WriteFileAtomic(path, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if got, _ := os.ReadFile(path); string(got) != "x" {
		t.Errorf("content after shrinking = %q", got)
	}
	assertNoTempFiles()

	// Empty content is allowed.
	empty := filepath.Join(dir, "empty.bin")
	if err := WriteFileAtomic(empty, nil, 0o644); err != nil {
		t.Fatal(err)
	}
	if fi, err := os.Stat(empty); err != nil || fi.Size() != 0 {
		t.Errorf("empty file: fi=%v err=%v", fi, err)
	}

	// A parent that is a regular file cannot be created.
	blocker := filepath.Join(dir, "blocker")
	if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := WriteFileAtomic(filepath.Join(blocker, "child.json"), []byte("x"), 0o644); err == nil {
		t.Error("WriteFileAtomic under a regular file succeeded, want error")
	}
}

// requestLog records the headers of every request a test server received.
type requestLog struct {
	mu      sync.Mutex
	headers []http.Header
}

func (l *requestLog) add(r *http.Request) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.headers = append(l.headers, r.Header.Clone())
}

func (l *requestLog) get(i int) http.Header {
	l.mu.Lock()
	defer l.mu.Unlock()
	if i >= len(l.headers) {
		return http.Header{}
	}
	return l.headers[i]
}

func (l *requestLog) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.headers)
}

const fetchDoc = `[{"id":"radio-1","name":"Radio 1","source":"https://stream.example.com/live.aac",` +
	`"start":"2026-01-31T18:00:00+01:00","end":"2026-01-31T19:00:00+01:00","key":"shows/radio-1.aac",` +
	`"codec":"aac","bitrate":128000,"headers":{"X-A":"b"},"startEarly":"15s","stopLate":30},` +
	`{"id":"broken","source":"nope","start":"2026-01-31T18:00:00Z","end":"2026-01-31T19:00:00Z"}]`

func TestFetcher_Fetch(t *testing.T) {
	ctx := context.Background()

	t.Run("ok, etag and 304", func(t *testing.T) {
		var log requestLog
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			log.add(r)
			if r.Header.Get("If-None-Match") == `"v1"` {
				w.WriteHeader(http.StatusNotModified)
				return
			}
			w.Header().Set("ETag", `"v1"`)
			w.Header().Set("Date", "Sat, 29 Aug 2026 10:00:00 GMT")
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(fetchDoc))
		}))
		defer srv.Close()

		f := NewFetcher(srv.URL, 5*time.Second, "X-Token: abc", "test-agent/1.0", "", nil)
		if f.CachePath != "" {
			t.Errorf("CachePath = %q, want empty when cacheDir is empty", f.CachePath)
		}
		if f.Client == nil || f.Client.Timeout != 5*time.Second {
			t.Errorf("Client = %+v, want timeout 5s", f.Client)
		}

		before := time.Now()
		s, err := f.Fetch(ctx)
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		if got := ids(s); !equalStrings(got, []string{"radio-1"}) {
			t.Errorf("ids = %v", got)
		}
		if len(s.Invalid) != 1 || s.Invalid[0].ID != "broken" {
			t.Errorf("Invalid = %+v", s.Invalid)
		}
		if s.ETag != `"v1"` {
			t.Errorf("ETag = %q", s.ETag)
		}
		if s.Origin != "url" {
			t.Errorf("Origin = %q, want url", s.Origin)
		}
		if want := time.Date(2026, 8, 29, 10, 0, 0, 0, time.UTC); !s.ServerDate.Equal(want) {
			t.Errorf("ServerDate = %v, want %v", s.ServerDate, want)
		}
		if s.FetchedAt.Before(before) || s.FetchedAt.After(time.Now()) {
			t.Errorf("FetchedAt = %v, want between %v and now", s.FetchedAt, before)
		}

		h := log.get(0)
		if h.Get("X-Token") != "abc" {
			t.Errorf("X-Token = %q, want abc", h.Get("X-Token"))
		}
		if h.Get("User-Agent") != "test-agent/1.0" {
			t.Errorf("User-Agent = %q", h.Get("User-Agent"))
		}
		if h.Get("Accept") != "application/json" {
			t.Errorf("Accept = %q", h.Get("Accept"))
		}
		if h.Get("Cache-Control") != "no-cache" {
			t.Errorf("Cache-Control = %q", h.Get("Cache-Control"))
		}
		if h.Get("If-None-Match") != "" {
			t.Errorf("first request carried If-None-Match %q", h.Get("If-None-Match"))
		}

		// Second fetch: the stored ETag is sent and the 304 becomes ErrNotModified.
		s2, err := f.Fetch(ctx)
		if !errors.Is(err, ErrNotModified) {
			t.Fatalf("second Fetch err = %v, want ErrNotModified", err)
		}
		if s2 != nil {
			t.Errorf("second Fetch schedule = %+v, want nil", s2)
		}
		if got := log.get(1).Get("If-None-Match"); got != `"v1"` {
			t.Errorf("second request If-None-Match = %q, want %q", got, `"v1"`)
		}
		if log.count() != 2 {
			t.Errorf("server saw %d requests, want 2", log.count())
		}
	})

	t.Run("304 without a known etag is an error", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusNotModified)
		}))
		defer srv.Close()
		f := NewFetcher(srv.URL, 5*time.Second, "", "", "", nil)
		_, err := f.Fetch(ctx)
		if err == nil || errors.Is(err, ErrNotModified) || !strings.Contains(err.Error(), "unexpected HTTP status 304") {
			t.Errorf("err = %v, want unexpected HTTP status 304", err)
		}
	})

	t.Run("http errors", func(t *testing.T) {
		for _, status := range []int{http.StatusInternalServerError, http.StatusNotFound, http.StatusUnauthorized, http.StatusBadGateway} {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(status)
				_, _ = w.Write([]byte(fetchDoc))
			}))
			f := NewFetcher(srv.URL, 5*time.Second, "", "", "", nil)
			s, err := f.Fetch(ctx)
			srv.Close()
			if err == nil || !strings.Contains(err.Error(), "unexpected HTTP status "+fmt.Sprint(status)) {
				t.Errorf("status %d: err = %v", status, err)
			}
			if s != nil {
				t.Errorf("status %d: schedule = %+v, want nil", status, s)
			}
		}
	})

	t.Run("invalid JSON", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"items": [`))
		}))
		defer srv.Close()
		f := NewFetcher(srv.URL, 5*time.Second, "", "", "", nil)
		if _, err := f.Fetch(ctx); err == nil || !strings.Contains(err.Error(), "invalid JSON") {
			t.Errorf("err = %v", err)
		}
	})

	t.Run("all items invalid", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`[{"id":"x"}]`))
		}))
		defer srv.Close()
		f := NewFetcher(srv.URL, 5*time.Second, "", "", "", nil)
		if _, err := f.Fetch(ctx); err == nil || !strings.Contains(err.Error(), "all 1 items are invalid") {
			t.Errorf("err = %v", err)
		}
	})

	t.Run("empty body", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		defer srv.Close()
		f := NewFetcher(srv.URL, 5*time.Second, "", "", "", nil)
		if _, err := f.Fetch(ctx); err == nil || !strings.Contains(err.Error(), "empty document") {
			t.Errorf("err = %v", err)
		}
	})

	t.Run("body larger than MaxDocumentSize", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			chunk := bytes.Repeat([]byte{' '}, 1<<20)
			remaining := MaxDocumentSize + 1
			for remaining > 0 {
				n := min(remaining, len(chunk))
				if _, err := w.Write(chunk[:n]); err != nil {
					return
				}
				remaining -= n
			}
		}))
		defer srv.Close()
		f := NewFetcher(srv.URL, 30*time.Second, "", "", "", nil)
		s, err := f.Fetch(ctx)
		if err == nil || !strings.Contains(err.Error(), fmt.Sprintf("document larger than %d bytes", MaxDocumentSize)) {
			t.Errorf("err = %v", err)
		}
		if s != nil {
			t.Errorf("schedule = %+v, want nil", s)
		}
	})

	t.Run("auth header without a colon is ignored", func(t *testing.T) {
		var log requestLog
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			log.add(r)
			_, _ = w.Write([]byte(`[]`))
		}))
		defer srv.Close()
		f := NewFetcher(srv.URL, 5*time.Second, "garbage", "", "", nil)
		if _, err := f.Fetch(ctx); err != nil {
			t.Fatal(err)
		}
		if _, ok := log.get(0)["Garbage"]; ok {
			t.Error("malformed auth header was sent")
		}
		if log.get(0).Get("User-Agent") == "" {
			// Go's default User-Agent is used when none is configured.
			t.Error("User-Agent is empty")
		}
	})

	t.Run("cancelled context", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`[]`))
		}))
		defer srv.Close()
		cctx, cancel := context.WithCancel(ctx)
		cancel()
		f := NewFetcher(srv.URL, 5*time.Second, "", "", "", nil)
		if _, err := f.Fetch(cctx); err == nil || !errors.Is(err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", err)
		}
	})

	t.Run("unreachable server", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {}))
		url := srv.URL
		srv.Close()
		f := NewFetcher(url, 2*time.Second, "", "", "", nil)
		if _, err := f.Fetch(ctx); err == nil || !strings.Contains(err.Error(), "request failed") {
			t.Errorf("err = %v", err)
		}
	})

	t.Run("naive timestamps use the fetcher location", func(t *testing.T) {
		loc, err := time.LoadLocation("Europe/Prague")
		if err != nil {
			t.Skipf("time zone unavailable: %v", err)
		}
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(arr(obj(`"id":"a"`, fSource, `"start":"2026-01-31T19:00:00"`, farEnd))))
		}))
		defer srv.Close()
		f := NewFetcher(srv.URL, 5*time.Second, "", "", "", loc)
		s, err := f.Fetch(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if want := time.Date(2026, 1, 31, 18, 0, 0, 0, time.UTC); !s.Items[0].Start.Equal(want) {
			t.Errorf("Start = %v, want %v", s.Items[0].Start, want)
		}
	})
}

func TestFetcher_Cache(t *testing.T) {
	ctx := context.Background()

	t.Run("fetch writes cache and LoadCache reads it back", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("ETag", `"v1"`)
			_, _ = w.Write([]byte(fetchDoc))
		}))
		defer srv.Close()
		cacheDir := filepath.Join(t.TempDir(), "cache") // does not exist yet
		f := NewFetcher(srv.URL, 5*time.Second, "", "", cacheDir, nil)
		if want := filepath.Join(cacheDir, "schedule.cache.json"); f.CachePath != want {
			t.Fatalf("CachePath = %q, want %q", f.CachePath, want)
		}

		s, err := f.Fetch(ctx)
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		raw, err := os.ReadFile(f.CachePath)
		if err != nil {
			t.Fatalf("cache file not written: %v", err)
		}
		var onDisk struct {
			Items     []json.RawMessage `json:"items"`
			FetchedAt time.Time         `json:"fetchedAt"`
			URL       string            `json:"url"`
		}
		if err := json.Unmarshal(raw, &onDisk); err != nil {
			t.Fatalf("cache is not JSON: %v\n%s", err, raw)
		}
		if len(onDisk.Items) != 1 || onDisk.URL != srv.URL || !onDisk.FetchedAt.Equal(s.FetchedAt) {
			t.Errorf("cache document = %+v", onDisk)
		}
		entries, _ := os.ReadDir(cacheDir)
		for _, e := range entries {
			if strings.Contains(e.Name(), ".tmp-") {
				t.Errorf("leftover temp file %q in cache dir", e.Name())
			}
		}

		// A second fetcher (fresh process) loads the cache.
		f2 := NewFetcher(srv.URL, 5*time.Second, "", "", cacheDir, nil)
		c, err := f2.LoadCache()
		if err != nil {
			t.Fatalf("LoadCache: %v", err)
		}
		if c.Origin != "cache" {
			t.Errorf("Origin = %q, want cache", c.Origin)
		}
		if !c.FetchedAt.Equal(s.FetchedAt) {
			t.Errorf("FetchedAt = %v, want %v", c.FetchedAt, s.FetchedAt)
		}
		if c.ETag != "" || !c.ServerDate.IsZero() {
			t.Errorf("cache carries ETag %q / ServerDate %v, want none", c.ETag, c.ServerDate)
		}
		// Rejected entries are cached too, so a restart cannot mistake a
		// present-but-invalid item for a removed one.
		if len(c.Invalid) != len(s.Invalid) || (len(c.Invalid) > 0 && (c.Invalid[0].ID != s.Invalid[0].ID || !c.Present(s.Invalid[0].ID))) {
			t.Errorf("cache Invalid = %+v, want %+v", c.Invalid, s.Invalid)
		}
		if len(c.Items) != 1 {
			t.Fatalf("cache Items = %+v, want 1", c.Items)
		}
		want, got := s.Items[0], c.Items[0]
		if got.ID != want.ID || got.Name != want.Name || got.Source != want.Source || got.Key != want.Key ||
			got.Codec != want.Codec || got.Bitrate != want.Bitrate || got.Type != want.Type {
			t.Errorf("cached item = %+v, want %+v", got, want)
		}
		if !got.Start.Equal(want.Start) || !got.End.Equal(want.End) {
			t.Errorf("cached Start/End = %v/%v, want %v/%v", got.Start, got.End, want.Start, want.End)
		}
		if got.Headers["X-A"] != "b" {
			t.Errorf("cached Headers = %v", got.Headers)
		}
		if got.StartEarly == nil || *got.StartEarly != *want.StartEarly || got.StopLate == nil || *got.StopLate != *want.StopLate {
			t.Errorf("cached StartEarly/StopLate = %v/%v, want %v/%v", got.StartEarly, got.StopLate, want.StartEarly, want.StopLate)
		}
	})

	t.Run("empty schedule round trips through the cache", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(`{"items":[]}`))
		}))
		defer srv.Close()
		f := NewFetcher(srv.URL, 5*time.Second, "", "", t.TempDir(), nil)
		s, err := f.Fetch(ctx)
		if err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		c, err := f.LoadCache()
		if err != nil {
			t.Fatalf("LoadCache: %v", err)
		}
		if c.Items == nil || len(c.Items) != 0 {
			t.Errorf("Items = %#v, want empty non-nil", c.Items)
		}
		if c.Origin != "cache" {
			t.Errorf("Origin = %q", c.Origin)
		}
		if !c.FetchedAt.Equal(s.FetchedAt) {
			t.Errorf("FetchedAt = %v, want %v", c.FetchedAt, s.FetchedAt)
		}
	})

	t.Run("LoadCache errors", func(t *testing.T) {
		f := NewFetcher("http://example.invalid/s.json", time.Second, "", "", "", nil)
		if _, err := f.LoadCache(); err == nil || !strings.Contains(err.Error(), "cache disabled") {
			t.Errorf("disabled: err = %v", err)
		}
		f = NewFetcher("http://example.invalid/s.json", time.Second, "", "", t.TempDir(), nil)
		if _, err := f.LoadCache(); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("missing file: err = %v, want not-exist", err)
		}
		if err := os.WriteFile(f.CachePath, []byte(`{"items":[{"id":"x"}]}`), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := f.LoadCache(); err == nil || !strings.Contains(err.Error(), "parse cache") {
			t.Errorf("corrupt items: err = %v", err)
		}
	})

	t.Run("cache write failure still returns the schedule", func(t *testing.T) {
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte(arr(valid("radio-1"))))
		}))
		defer srv.Close()
		blocker := filepath.Join(t.TempDir(), "blocker")
		if err := os.WriteFile(blocker, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		f := NewFetcher(srv.URL, 5*time.Second, "", "", "", nil)
		f.CachePath = filepath.Join(blocker, "schedule.cache.json") // parent is a regular file
		s, err := f.Fetch(ctx)
		var ce *CacheError
		if !errors.As(err, &ce) {
			t.Fatalf("err = %v, want *CacheError", err)
		}
		if !strings.HasPrefix(err.Error(), "write schedule cache: ") || ce.Unwrap() == nil {
			t.Errorf("CacheError = %q, unwrap = %v", err, ce.Unwrap())
		}
		if s == nil || len(s.Items) != 1 || s.Origin != "url" {
			t.Errorf("schedule alongside CacheError = %+v, want the parsed schedule", s)
		}
	})
}
