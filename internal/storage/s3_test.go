package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"unicode/utf8"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/spectado/stream-recorder/internal/config"
	"github.com/spectado/stream-recorder/internal/recorder"
)

func apiErr(code string) error {
	return &smithy.GenericAPIError{Code: code, Message: "test " + code}
}

func respErr(status int, inner error) error {
	return &awshttp.ResponseError{
		ResponseError: &smithyhttp.ResponseError{
			Response: &smithyhttp.Response{Response: &http.Response{StatusCode: status}},
			Err:      inner,
		},
		RequestID: "req-1",
	}
}

func opErr(inner error) error {
	return &smithy.OperationError{ServiceID: "S3", OperationName: "PutObject", Err: inner}
}

// timeoutErr is a net.Error the way the standard library reports timeouts.
type timeoutErr struct{}

func (timeoutErr) Error() string   { return "i/o timeout" }
func (timeoutErr) Timeout() bool   { return true }
func (timeoutErr) Temporary() bool { return true }

var _ net.Error = timeoutErr{}

func isPermanent(err error) bool {
	var p *recorder.PermanentError
	return errors.As(err, &p)
}

func TestClassify(t *testing.T) {
	if got := classify(nil); got != nil {
		t.Fatalf("classify(nil) = %v, want nil", got)
	}

	transient := []struct {
		name string
		err  error
	}{
		{"context canceled", context.Canceled},
		{"wrapped context canceled", fmt.Errorf("upload: %w", context.Canceled)},
		{"deadline exceeded", context.DeadlineExceeded},
		{"wrapped deadline exceeded", opErr(fmt.Errorf("send: %w", context.DeadlineExceeded))},
		{"net.OpError", &net.OpError{Op: "dial", Net: "tcp", Err: errors.New("connection refused")}},
		{"wrapped net.OpError", fmt.Errorf("put: %w", &net.OpError{Op: "read", Net: "tcp", Err: errors.New("reset")})},
		{"timeout net.Error", timeoutErr{}},
		{"wrapped timeout net.Error", opErr(fmt.Errorf("send: %w", timeoutErr{}))},
		{"DNS error", &net.DNSError{Err: "no such host", Name: "r2.example", IsNotFound: true}},
		{"plain error", errors.New("something odd")},
		{"SlowDown", fmt.Errorf("upload: %w", apiErr("SlowDown"))},
		{"InternalError", apiErr("InternalError")},
		{"ServiceUnavailable", apiErr("ServiceUnavailable")},
		{"RequestTimeout", apiErr("RequestTimeout")},
		{"status 503", respErr(http.StatusServiceUnavailable, errors.New("unavailable"))},
		{"status 500", respErr(http.StatusInternalServerError, errors.New("boom"))},
		{"status 429", respErr(http.StatusTooManyRequests, errors.New("slow down"))},
		{"status 200 with unknown code", respErr(http.StatusOK, apiErr("Weird"))},
		{"realistic SlowDown 503", opErr(respErr(http.StatusServiceUnavailable, apiErr("SlowDown")))},
		{"already permanent stays as is", &recorder.PermanentError{Err: errors.New("x")}},
		// Bare 400/404 (no permanent error code) are transient by design:
		// RequestTimeout, IncompleteBody, NoSuchUpload... all use them.
		{"status 400 without code", respErr(http.StatusBadRequest, errors.New("bad"))},
		{"status 404 without code", respErr(http.StatusNotFound, errors.New("missing"))},
		{"unknown code with 400 status", opErr(respErr(http.StatusBadRequest, apiErr("SomethingNew")))},
		{"NoSuchUpload 404", opErr(respErr(http.StatusNotFound, apiErr("NoSuchUpload")))},
		{"BadDigest 400", opErr(respErr(http.StatusBadRequest, apiErr("BadDigest")))},
		{"IncompleteBody 400", opErr(respErr(http.StatusBadRequest, apiErr("IncompleteBody")))},
	}
	for _, tc := range transient {
		t.Run("transient/"+tc.name, func(t *testing.T) {
			got := classify(tc.err)
			if got != tc.err {
				t.Errorf("classify(%v) = %v (%T), want the same error back unwrapped", tc.err, got, got)
			}
			if _, alreadyPermanent := tc.err.(*recorder.PermanentError); !alreadyPermanent && isPermanent(got) {
				t.Errorf("classify(%v) is a PermanentError, want transient", tc.err)
			}
		})
	}

	permanent := []struct {
		name string
		err  error
	}{
		{"AccessDenied wrapped", fmt.Errorf("upload: %w", apiErr("AccessDenied"))},
		{"AccessDenied bare", apiErr("AccessDenied")},
		{"InvalidAccessKeyId", apiErr("InvalidAccessKeyId")},
		{"SignatureDoesNotMatch", apiErr("SignatureDoesNotMatch")},
		{"NoSuchBucket", apiErr("NoSuchBucket")},
		{"InvalidBucketName", apiErr("InvalidBucketName")},
		{"MetadataTooLarge", apiErr("MetadataTooLarge")},
		{"InvalidArgument", apiErr("InvalidArgument")},
		{"EntityTooLarge", apiErr("EntityTooLarge")},
		{"KeyTooLongError", apiErr("KeyTooLongError")},
		{"InvalidRequest", apiErr("InvalidRequest")},
		{"Unauthorized", apiErr("Unauthorized")},
		{"Forbidden", apiErr("Forbidden")},
		{"AuthorizationHeaderMalformed", apiErr("AuthorizationHeaderMalformed")},
		{"status 403", respErr(http.StatusForbidden, errors.New("forbidden"))},
		{"status 401", respErr(http.StatusUnauthorized, errors.New("unauthorized"))},
		{"realistic AccessDenied 403", opErr(respErr(http.StatusForbidden, apiErr("AccessDenied")))},
		{"permanent code with retryable status", opErr(respErr(http.StatusServiceUnavailable, apiErr("NoSuchBucket")))},
	}
	for _, tc := range permanent {
		t.Run("permanent/"+tc.name, func(t *testing.T) {
			got := classify(tc.err)
			if !isPermanent(got) {
				t.Fatalf("classify(%v) = %v (%T), want *recorder.PermanentError", tc.err, got, got)
			}
			if got.Error() != tc.err.Error() {
				t.Errorf("message changed: %q vs %q", got.Error(), tc.err.Error())
			}
			if !errors.Is(got, tc.err) {
				t.Errorf("PermanentError does not unwrap to the original error")
			}
			var ae smithy.APIError
			if errors.As(tc.err, &ae) {
				var got2 smithy.APIError
				if !errors.As(got, &got2) || got2.ErrorCode() != ae.ErrorCode() {
					t.Errorf("API error code lost through classify: %v", got)
				}
			}
		})
	}
}

func TestErrorCodeAndStatus(t *testing.T) {
	cases := []struct {
		name       string
		err        error
		wantCode   string
		wantStatus int
	}{
		{"nil", nil, "", 0},
		{"plain error", errors.New("x"), "", 0},
		{"api error only", apiErr("AccessDenied"), "AccessDenied", 0},
		{"wrapped api error", fmt.Errorf("a: %w", fmt.Errorf("b: %w", apiErr("SlowDown"))), "SlowDown", 0},
		{"response error only", respErr(http.StatusServiceUnavailable, errors.New("x")), "", 503},
		{"response error wrapping api error", respErr(http.StatusForbidden, apiErr("AccessDenied")), "AccessDenied", 403},
		{"operation error chain", opErr(respErr(http.StatusNotFound, apiErr("NoSuchBucket"))), "NoSuchBucket", 404},
		{"context canceled", context.Canceled, "", 0},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, status := errorCodeAndStatus(tc.err)
			if code != tc.wantCode || status != tc.wantStatus {
				t.Errorf("errorCodeAndStatus(%v) = (%q, %d), want (%q, %d)", tc.err, code, status, tc.wantCode, tc.wantStatus)
			}
		})
	}
}

func TestSanitizeMetadata(t *testing.T) {
	t.Run("nil and empty", func(t *testing.T) {
		if got := SanitizeMetadata(nil); got != nil {
			t.Errorf("SanitizeMetadata(nil) = %v, want nil", got)
		}
		if got := SanitizeMetadata(map[string]string{}); got != nil {
			t.Errorf("SanitizeMetadata(empty) = %v, want nil", got)
		}
	})

	t.Run("keys", func(t *testing.T) {
		cases := []struct {
			in   string
			want string // "" means dropped
		}{
			{"title", "title"},
			{"Content-Title", "content-title"},
			{"MY_KEY", "my-key"},
			{"my key", "my-key"},
			{"my.key", "my-key"},
			{"a_b.c d", "a-b-c-d"},
			{"k€y!", "ky"},
			{"__x__", "x"},
			{"-lead-and-trail-", "lead-and-trail"},
			{"Ünïcode", "ncode"},
			{"x-amz-meta-title", "x-amz-meta-title"},
			{"!!!", ""},
			{"", ""},
			{"   ", ""},
			{"日本", ""},
		}
		for _, tc := range cases {
			got := SanitizeMetadata(map[string]string{tc.in: "v"})
			if tc.want == "" {
				if len(got) != 0 {
					t.Errorf("key %q: got %v, want dropped", tc.in, got)
				}
				continue
			}
			if len(got) != 1 || got[tc.want] != "v" {
				t.Errorf("key %q: got %v, want {%q: v}", tc.in, got, tc.want)
			}
		}
	})

	t.Run("ascii values", func(t *testing.T) {
		in := map[string]string{
			"plain":     "Radio One - Morning Show",
			"padded":    "  spaced  ",
			"control":   "a\x00b\tc\nd\re\x7ff",
			"only-ctl":  "\x00\x01\x1f\x7f",
			"empty":     "",
			"spaces":    "    ",
			"punct":     "~!@#$%^&*()_+{}|:\"<>?`-=[]\\;',./",
			"high-edge": "\x7e",
		}
		got := SanitizeMetadata(in)
		want := map[string]string{
			"plain":     "Radio One - Morning Show",
			"padded":    "spaced",
			"control":   "abcdef",
			"punct":     "~!@#$%^&*()_+{}|:\"<>?`-=[]\\;',./",
			"high-edge": "\x7e",
		}
		if len(got) != len(want) {
			t.Errorf("got %d entries %v, want %d %v", len(got), got, len(want), want)
		}
		for k, v := range want {
			if got[k] != v {
				t.Errorf("%s = %q, want %q", k, got[k], v)
			}
		}
		for _, k := range []string{"only-ctl", "empty", "spaces"} {
			if _, ok := got[k]; ok {
				t.Errorf("%s was kept with value %q, want dropped", k, got[k])
			}
		}
	})

	t.Run("non-ascii values are RFC 2047 encoded", func(t *testing.T) {
		cases := []string{"Český rozhlas", "Ünïcödé ✓", "日本語のタイトル", "é", "mixed ascii and ünïcode"}
		dec := new(mime.WordDecoder)
		for _, v := range cases {
			got := SanitizeMetadata(map[string]string{"title": v})["title"]
			if !strings.HasPrefix(got, "=?utf-8?b?") || !strings.HasSuffix(got, "?=") {
				t.Errorf("%q -> %q, want an =?utf-8?b?...?= encoded word", v, got)
			}
			for _, r := range got {
				if r > 0x7e || r < 0x20 {
					t.Errorf("%q -> %q contains non-ASCII byte %U", v, got, r)
					break
				}
			}
			back, err := dec.DecodeHeader(got)
			if err != nil {
				t.Errorf("decode %q: %v", got, err)
				continue
			}
			if back != v {
				t.Errorf("%q round-tripped to %q", v, back)
			}
		}
	})

	t.Run("non-ascii control chars stripped before encoding", func(t *testing.T) {
		got := SanitizeMetadata(map[string]string{"t": "Čes\x00ký\n"})["t"]
		back, err := new(mime.WordDecoder).DecodeHeader(got)
		if err != nil || back != "Český" {
			t.Errorf("got %q -> %q (%v), want Český", got, back, err)
		}
	})

	t.Run("long values are truncated to 256 bytes", func(t *testing.T) {
		got := SanitizeMetadata(map[string]string{"v": strings.Repeat("a", 300)})["v"]
		if len(got) != 256 || strings.Trim(got, "a") != "" {
			t.Errorf("len = %d, want 256 of 'a'", len(got))
		}
		got = SanitizeMetadata(map[string]string{"v": strings.Repeat("b", 256)})["v"]
		if len(got) != 256 {
			t.Errorf("exactly 256 bytes: len = %d", len(got))
		}
	})

	t.Run("long multibyte values are cut on a rune boundary", func(t *testing.T) {
		// 86 x "€" (3 bytes each) = 258 bytes; 256 is not a rune start, so 255 bytes = 85 runes remain.
		got := SanitizeMetadata(map[string]string{"v": strings.Repeat("€", 86)})["v"]
		back, err := new(mime.WordDecoder).DecodeHeader(got)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !utf8.ValidString(back) {
			t.Fatalf("decoded value is not valid UTF-8: %q", back)
		}
		if back != strings.Repeat("€", 85) {
			t.Errorf("decoded %d bytes / %d runes, want 85 runes", len(back), utf8.RuneCountInString(back))
		}

		// 129 x "é" (2 bytes) = 258 bytes; 256 is a rune start, so exactly 128 runes remain.
		got = SanitizeMetadata(map[string]string{"v": strings.Repeat("é", 129)})["v"]
		back, err = new(mime.WordDecoder).DecodeHeader(got)
		if err != nil || back != strings.Repeat("é", 128) {
			t.Errorf("decoded %d runes (%v), want 128", utf8.RuneCountInString(back), err)
		}
	})

	t.Run("total size cap drops extra entries", func(t *testing.T) {
		in := make(map[string]string, 20)
		for i := 0; i < 20; i++ {
			in[fmt.Sprintf("k%02d", i)] = strings.Repeat("v", 200)
		}
		got := SanitizeMetadata(in) // must not panic
		// Every entry costs 3 + 200 bytes; floor(1800 / 203) = 8 fit.
		if len(got) != 8 {
			t.Errorf("kept %d entries, want 8", len(got))
		}
		total := 0
		for k, v := range got {
			total += len(k) + len(v)
			if _, ok := in[k]; !ok {
				t.Errorf("unexpected key %q", k)
			}
		}
		if total > 1800 {
			t.Errorf("total size %d exceeds 1800", total)
		}
	})

	t.Run("smaller entries still fit after a large one is skipped", func(t *testing.T) {
		in := map[string]string{}
		for i := 0; i < 8; i++ {
			in[fmt.Sprintf("big%d", i)] = strings.Repeat("x", 220) // 224 each: 7 fit (1568), the 8th (1792) fits too, total 1792
		}
		in["tiny"] = "y" // 5 bytes: 1797 <= 1800 fits
		got := SanitizeMetadata(in)
		if _, ok := got["tiny"]; !ok {
			t.Errorf("tiny entry dropped: %v", got)
		}
		if len(got) != 9 {
			t.Errorf("kept %d entries, want 9", len(got))
		}
	})

	t.Run("colliding sanitized keys keep exactly one", func(t *testing.T) {
		got := SanitizeMetadata(map[string]string{"a_b": "1", "a.b": "2", "A B": "3"})
		if len(got) != 1 {
			t.Errorf("got %v, want a single a-b entry", got)
		}
		if v := got["a-b"]; v != "1" && v != "2" && v != "3" {
			t.Errorf("a-b = %q", v)
		}
	})

	t.Run("input map is not modified", func(t *testing.T) {
		in := map[string]string{"My_Key": "Český"}
		_ = SanitizeMetadata(in)
		if len(in) != 1 || in["My_Key"] != "Český" {
			t.Errorf("input mutated: %v", in)
		}
	})
}

func TestTruncateUTF8(t *testing.T) {
	cases := []struct {
		s    string
		max  int
		want string
	}{
		{"hello", 10, "hello"},
		{"hello", 5, "hello"},
		{"hello", 3, "hel"},
		{"hello", 0, ""},
		{"", 5, ""},
		{"", 0, ""},
		{"héllo", 1, "h"},
		{"héllo", 2, "h"},  // byte 2 is the continuation byte of é
		{"héllo", 3, "hé"}, // byte 3 starts 'l'
		{"héllo", 4, "hél"},
		{"€€", 6, "€€"},
		{"€€", 5, "€"},
		{"€€", 4, "€"},
		{"€€", 3, "€"},
		{"€€", 2, ""},
		{"€€", 1, ""},
		{"a€", 2, "a"},
		{"a€", 3, "a"},
		{"a€", 4, "a€"},
		{"日本語", 7, "日本"},
		{"日本語", 6, "日本"},
	}
	for _, tc := range cases {
		got := truncateUTF8(tc.s, tc.max)
		if got != tc.want {
			t.Errorf("truncateUTF8(%q, %d) = %q, want %q", tc.s, tc.max, got, tc.want)
		}
		if len(got) > tc.max {
			t.Errorf("truncateUTF8(%q, %d) = %d bytes, exceeds max", tc.s, tc.max, len(got))
		}
		if !utf8.ValidString(got) {
			t.Errorf("truncateUTF8(%q, %d) = %q is not valid UTF-8", tc.s, tc.max, got)
		}
	}
}

func TestSanitizeKey(t *testing.T) {
	cases := map[string]string{
		"":                 "",
		"title":            "title",
		"Content-Type":     "content-type",
		"ABC123":           "abc123",
		"my_key":           "my-key",
		"my key":           "my-key",
		"my.key":           "my-key",
		"a_b.c d-e":        "a-b-c-d-e",
		"__x__":            "x",
		"-lead-":           "lead",
		"...":              "",
		"!!!":              "",
		"k€y!":             "ky",
		"Ünïcode":          "ncode",
		"tab\tkey":         "tabkey",
		"new\nline":        "newline",
		"x-amz-meta-title": "x-amz-meta-title",
		"a--b":             "a--b",
		"a  b":             "a--b",
	}
	for in, want := range cases {
		if got := sanitizeKey(in); got != want {
			t.Errorf("sanitizeKey(%q) = %q, want %q", in, got, want)
		}
	}
}

// ---------------------------------------------------------------------------
// fakeBucket: an S3 endpoint with the pieces the recorder depends on
// ---------------------------------------------------------------------------

type fakeObject struct {
	data        []byte
	meta        map[string]string
	contentType string
	etag        string // without quotes, as ObjectInfo carries it
}

type fakeUpload struct {
	key         string
	meta        map[string]string
	contentType string
	parts       map[int][]byte
}

// fakeBucket is a small S3 endpoint under /bucket/<key>: whole-object
// PUT/GET/HEAD/DELETE with user metadata and ETags, If-None-Match/If-Match
// preconditions, ranged GETs (what manager.Downloader issues) and the
// multipart trio the conditional-write probe exercises.
type fakeBucket struct {
	mu       sync.Mutex
	objects  map[string]*fakeObject
	uploads  map[string]*fakeUpload
	puts     int // object-creating PUTs (parts do not count)
	requests int
	seq      int // makes every stored version's ETag unique

	// ignoreIfMatch models an endpoint that accepts If-Match and silently
	// stores the object anyway — the reason ConditionalWrites exists.
	ignoreIfMatch bool
	// ignoreCompleteIfMatch models the subtler half of that: preconditions
	// work on PutObject but are dropped on CompleteMultipartUpload, which is
	// the path every recording above UPLOAD_PART_SIZE takes.
	ignoreCompleteIfMatch bool
}

func (b *fakeBucket) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	key := strings.TrimPrefix(r.URL.Path, "/bucket/")
	q := r.URL.Query()
	b.mu.Lock()
	defer b.mu.Unlock()
	b.requests++

	switch {
	case r.Method == http.MethodPost && q.Has("uploads"):
		b.seq++
		id := fmt.Sprintf("upload-%d", b.seq)
		b.uploads[id] = &fakeUpload{
			key: key, meta: metaHeaders(r), contentType: r.Header.Get("Content-Type"),
			parts: map[int][]byte{},
		}
		writeXML(w, http.StatusOK, fmt.Sprintf(
			`<InitiateMultipartUploadResult><Bucket>bucket</Bucket><Key>%s</Key><UploadId>%s</UploadId></InitiateMultipartUploadResult>`, key, id))

	case r.Method == http.MethodPut && q.Get("uploadId") != "":
		u, ok := b.uploads[q.Get("uploadId")]
		if !ok {
			writeS3Error(w, http.StatusNotFound, "NoSuchUpload")
			return
		}
		n, _ := strconv.Atoi(q.Get("partNumber"))
		data, _ := io.ReadAll(r.Body)
		u.parts[n] = data
		w.Header().Set("ETag", fmt.Sprintf(`"part-%d"`, n))
		w.WriteHeader(http.StatusOK)

	case r.Method == http.MethodPost && q.Get("uploadId") != "":
		id := q.Get("uploadId")
		u, ok := b.uploads[id]
		if !ok {
			writeS3Error(w, http.StatusNotFound, "NoSuchUpload")
			return
		}
		_, _ = io.ReadAll(r.Body) // the part list; the parts are already here
		if !b.ignoreCompleteIfMatch && !b.precondition(w, r, key) {
			return // the upload stays open so the client can abort it
		}
		delete(b.uploads, id)
		nums := make([]int, 0, len(u.parts))
		for n := range u.parts {
			nums = append(nums, n)
		}
		sort.Ints(nums)
		var body []byte
		for _, n := range nums {
			body = append(body, u.parts[n]...)
		}
		obj := b.store(key, body, u.meta, u.contentType)
		b.puts++
		writeXML(w, http.StatusOK, fmt.Sprintf(
			`<CompleteMultipartUploadResult><Bucket>bucket</Bucket><Key>%s</Key><ETag>&quot;%s&quot;</ETag></CompleteMultipartUploadResult>`, key, obj.etag))

	case r.Method == http.MethodDelete && q.Get("uploadId") != "":
		delete(b.uploads, q.Get("uploadId"))
		w.WriteHeader(http.StatusNoContent)

	case r.Method == http.MethodDelete:
		delete(b.objects, key)
		w.WriteHeader(http.StatusNoContent)

	case r.Method == http.MethodPut:
		if !b.precondition(w, r, key) {
			return
		}
		data, _ := io.ReadAll(r.Body)
		obj := b.store(key, data, metaHeaders(r), r.Header.Get("Content-Type"))
		b.puts++
		w.Header().Set("ETag", `"`+obj.etag+`"`)
		w.WriteHeader(http.StatusOK)

	case r.Method == http.MethodHead, r.Method == http.MethodGet:
		obj, ok := b.objects[key]
		if !ok {
			writeS3Error(w, http.StatusNotFound, "NoSuchKey")
			return
		}
		if im := r.Header.Get("If-Match"); im != "" && im != `"`+obj.etag+`"` {
			writeS3Error(w, http.StatusPreconditionFailed, "PreconditionFailed")
			return
		}
		w.Header().Set("ETag", `"`+obj.etag+`"`)
		if obj.contentType != "" {
			w.Header().Set("Content-Type", obj.contentType)
		}
		for k, v := range obj.meta {
			w.Header().Set("x-amz-meta-"+k, v)
		}
		data := obj.data
		status := http.StatusOK
		if rng := r.Header.Get("Range"); rng != "" && r.Method == http.MethodGet {
			start, end, ok := parseByteRange(rng, len(obj.data))
			if !ok {
				writeS3Error(w, http.StatusRequestedRangeNotSatisfiable, "InvalidRange")
				return
			}
			data = obj.data[start : end+1]
			w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(obj.data)))
			status = http.StatusPartialContent
		}
		w.Header().Set("Content-Length", strconv.Itoa(len(data)))
		w.WriteHeader(status)
		if r.Method == http.MethodGet {
			_, _ = w.Write(data)
		}

	default:
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

// precondition applies If-None-Match/If-Match; it answers 412 and reports
// false when the request must not store anything.
func (b *fakeBucket) precondition(w http.ResponseWriter, r *http.Request, key string) bool {
	obj, exists := b.objects[key]
	if r.Header.Get("If-None-Match") == "*" && exists {
		writeS3Error(w, http.StatusPreconditionFailed, "PreconditionFailed")
		return false
	}
	if im := r.Header.Get("If-Match"); im != "" && !b.ignoreIfMatch {
		if !exists || im != `"`+obj.etag+`"` {
			writeS3Error(w, http.StatusPreconditionFailed, "PreconditionFailed")
			return false
		}
	}
	return true
}

func (b *fakeBucket) store(key string, data []byte, meta map[string]string, contentType string) *fakeObject {
	b.seq++
	obj := &fakeObject{data: data, meta: meta, contentType: contentType, etag: fmt.Sprintf("etag-%d", b.seq)}
	b.objects[key] = obj
	return obj
}

func (b *fakeBucket) object(key string) (*fakeObject, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	obj, ok := b.objects[key]
	return obj, ok
}

func (b *fakeBucket) keys() []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]string, 0, len(b.objects))
	for k := range b.objects {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func (b *fakeBucket) stats() (puts, requests, uploads int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.puts, b.requests, len(b.uploads)
}

func metaHeaders(r *http.Request) map[string]string {
	var out map[string]string
	for k, v := range r.Header {
		if len(v) == 0 || !strings.HasPrefix(strings.ToLower(k), "x-amz-meta-") {
			continue
		}
		if out == nil {
			out = map[string]string{}
		}
		out[strings.ToLower(strings.TrimPrefix(strings.ToLower(k), "x-amz-meta-"))] = v[0]
	}
	return out
}

// parseByteRange understands the "bytes=start-end" form the SDK's downloader
// sends; end beyond the object is clamped, start beyond it is a 416.
func parseByteRange(v string, size int) (start, end int, ok bool) {
	spec, found := strings.CutPrefix(v, "bytes=")
	if !found {
		return 0, 0, false
	}
	lo, hi, found := strings.Cut(spec, "-")
	if !found {
		return 0, 0, false
	}
	start, err := strconv.Atoi(lo)
	if err != nil || start >= size {
		return 0, 0, false
	}
	end = size - 1
	if hi != "" {
		if end, err = strconv.Atoi(hi); err != nil {
			return 0, 0, false
		}
		if end > size-1 {
			end = size - 1
		}
	}
	if end < start {
		return 0, 0, false
	}
	return start, end, true
}

func writeS3Error(w http.ResponseWriter, status int, code string) {
	writeXML(w, status, fmt.Sprintf(`<Error><Code>%s</Code><Message>%s</Message></Error>`, code, code))
}

func writeXML(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(status)
	fmt.Fprint(w, body)
}

func newTestS3(t *testing.T) (*S3, *fakeBucket) {
	return newTestS3With(t, nil)
}

// newTestS3With lets a test shape the endpoint and the configuration before
// the server accepts anything (nothing may race with a live handler).
func newTestS3With(t *testing.T, setup func(*fakeBucket, *config.Config)) (*S3, *fakeBucket) {
	t.Helper()
	b := &fakeBucket{objects: map[string]*fakeObject{}, uploads: map[string]*fakeUpload{}}
	cfg := &config.Config{
		S3Region: "auto", S3Bucket: "bucket",
		S3AccessKeyID: "k", S3SecretAccessKey: "s", S3ForcePathStyle: true,
		S3ConditionalPut: true, UploadPartSize: 5 * 1024 * 1024,
	}
	if setup != nil {
		setup(b, cfg)
	}
	srv := httptest.NewServer(b)
	t.Cleanup(srv.Close)
	cfg.S3Endpoint = srv.URL
	s, err := NewS3(context.Background(), cfg, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return s, b
}

func writeFile(t *testing.T, path string, data []byte) string {
	t.Helper()
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestUploadVerifiesAndTreatsSameSizeConflictAsDone(t *testing.T) {
	s, b := newTestS3(t)
	ctx := context.Background()
	p := filepath.Join(t.TempDir(), "a.m4a")
	writeFile(t, p, []byte("0123456789"))

	etag, err := s.Upload(ctx, p, "2026-09-03/x.m4a", "audio/mp4", map[string]string{"name": "x"}, "")
	if err != nil {
		t.Fatalf("upload: %v", err)
	}
	obj, ok := b.object("2026-09-03/x.m4a")
	if !ok || etag != obj.etag || etag == "" {
		t.Fatalf("etag=%q stored=%v", etag, ok)
	}
	// Second attempt (retry after a crash): the conditional put is refused,
	// the sizes match, so the upload counts as done.
	if _, err := s.Upload(ctx, p, "2026-09-03/x.m4a", "audio/mp4", nil, ""); err != nil {
		t.Fatalf("retry with identical object: %v", err)
	}
	// A different object under the same key is a conflict.
	writeFile(t, p, []byte("01234567890123"))
	if _, err := s.Upload(ctx, p, "2026-09-03/x.m4a", "audio/mp4", nil, ""); !errors.Is(err, recorder.ErrObjectExists) {
		t.Fatalf("different object: err=%v, want ErrObjectExists", err)
	}
	if puts, _, _ := b.stats(); puts != 1 {
		t.Fatalf("puts=%d, want 1", puts)
	}
}

func TestUploadReplaceWithIfMatch(t *testing.T) {
	s, b := newTestS3(t)
	ctx := context.Background()
	dir := t.TempDir()
	key := "2026-09-22/match-ro-jpOkle8Mp0.m4a"

	first := writeFile(t, filepath.Join(dir, "first.m4a"), []byte("part-one"))
	etag1, err := s.Upload(ctx, first, key, "audio/mp4", map[string]string{"parts": "1"}, "")
	if err != nil {
		t.Fatalf("first upload: %v", err)
	}

	// Merging: the second session replaces exactly the object the HEAD saw.
	merged := writeFile(t, filepath.Join(dir, "merged.m4a"), []byte("part-one+part-two"))
	etag2, err := s.Upload(ctx, merged, key, "audio/mp4", map[string]string{"parts": "2"}, etag1)
	if err != nil {
		t.Fatalf("replace with the current etag: %v", err)
	}
	if etag2 == "" || etag2 == etag1 {
		t.Fatalf("etag after replace = %q (was %q)", etag2, etag1)
	}
	obj, _ := b.object(key)
	if string(obj.data) != "part-one+part-two" || obj.meta["parts"] != "2" {
		t.Fatalf("stored %q meta=%v", obj.data, obj.meta)
	}

	// A stale ETag means somebody else wrote the object in between.
	if _, err := s.Upload(ctx, merged, key, "audio/mp4", nil, etag1); !errors.Is(err, recorder.ErrObjectChanged) {
		t.Fatalf("stale etag: err=%v, want ErrObjectChanged", err)
	}
	if obj, _ := b.object(key); string(obj.data) != "part-one+part-two" {
		t.Fatalf("a refused replace changed the object: %q", obj.data)
	}

	// A first (non-replacing) upload of a different object still reports the
	// conflict rather than a changed object.
	other := writeFile(t, filepath.Join(dir, "other.m4a"), []byte("something else entirely"))
	if _, err := s.Upload(ctx, other, key, "audio/mp4", nil, ""); !errors.Is(err, recorder.ErrObjectExists) {
		t.Fatalf("conditional put on a taken key: err=%v, want ErrObjectExists", err)
	}
}

// TestUploadMultipartCarriesIfMatch covers the path a real recording takes:
// above UPLOAD_PART_SIZE the SDK switches to multipart and the condition has
// to reach CompleteMultipartUpload.
func TestUploadMultipartCarriesIfMatch(t *testing.T) {
	s, b := newTestS3(t)
	ctx := context.Background()
	dir := t.TempDir()
	key := "2026-09-22/long.m4a"
	big := writeFile(t, filepath.Join(dir, "big.m4a"), bytes.Repeat([]byte("a"), 6*1024*1024))

	etag1, err := s.Upload(ctx, big, key, "audio/mp4", map[string]string{"parts": "1"}, "")
	if err != nil {
		t.Fatalf("multipart upload: %v", err)
	}
	bigger := writeFile(t, filepath.Join(dir, "bigger.m4a"), bytes.Repeat([]byte("b"), 7*1024*1024))
	if _, err := s.Upload(ctx, bigger, key, "audio/mp4", nil, "etag-does-not-match"); !errors.Is(err, recorder.ErrObjectChanged) {
		t.Fatalf("multipart replace with a stale etag: err=%v, want ErrObjectChanged", err)
	}
	if obj, _ := b.object(key); len(obj.data) != 6*1024*1024 {
		t.Fatalf("refused multipart replace stored %d bytes", len(obj.data))
	}
	if _, _, uploads := b.stats(); uploads != 0 {
		t.Fatalf("%d multipart uploads left dangling", uploads)
	}
	if _, err := s.Upload(ctx, bigger, key, "audio/mp4", nil, etag1); err != nil {
		t.Fatalf("multipart replace with the current etag: %v", err)
	}
	if obj, _ := b.object(key); len(obj.data) != 7*1024*1024 {
		t.Fatalf("after replace: %d bytes", len(obj.data))
	}
}

func TestHeadAndDownload(t *testing.T) {
	s, b := newTestS3(t)
	ctx := context.Background()
	dir := t.TempDir()
	key := "2026-09-22/match-ro-jpOkle8Mp0.m4a"

	if _, found, err := s.Head(ctx, key); found || err != nil {
		t.Fatalf("head of a missing object: found=%v err=%v", found, err)
	}

	payload := bytes.Repeat([]byte("m4a"), 5000) // 15000 bytes, one ranged GET
	src := writeFile(t, filepath.Join(dir, "src.m4a"), payload)
	etag, err := s.Upload(ctx, src, key, "audio/mp4", map[string]string{
		"recording-id": "match-ro-jpOkle8Mp0",
		"sessions":     "a1b2c3d4,e5f60718",
		"parts":        "2",
		"name":         "Ranní show",
	}, "")
	if err != nil {
		t.Fatal(err)
	}

	info, found, err := s.Head(ctx, key)
	if err != nil || !found {
		t.Fatalf("head: found=%v err=%v", found, err)
	}
	if info.Size != int64(len(payload)) || info.ETag != etag || strings.Contains(info.ETag, `"`) {
		t.Fatalf("head = %+v, want size %d etag %q unquoted", info, len(payload), etag)
	}
	if info.Metadata["recording-id"] != "match-ro-jpOkle8Mp0" || info.Metadata["sessions"] != "a1b2c3d4,e5f60718" ||
		info.Metadata["parts"] != "2" {
		t.Fatalf("metadata = %v", info.Metadata)
	}
	name, err := new(mime.WordDecoder).DecodeHeader(info.Metadata["name"])
	if err != nil || name != "Ranní show" {
		t.Fatalf("name = %q -> %q (%v)", info.Metadata["name"], name, err)
	}

	out := filepath.Join(dir, "got.m4a")
	if err := s.Download(ctx, key, info.ETag, out); err != nil {
		t.Fatalf("download: %v", err)
	}
	got, err := os.ReadFile(out)
	if err != nil || !bytes.Equal(got, payload) {
		t.Fatalf("downloaded %d bytes (err %v), want %d", len(got), err, len(payload))
	}
	if _, err := os.Stat(out + ".part"); !os.IsNotExist(err) {
		t.Fatalf("partial file left behind: %v", err)
	}

	// The object changed since the HEAD that classified it: the merge must not
	// be built on it.
	stale := filepath.Join(dir, "stale.m4a")
	if err := s.Download(ctx, key, "etag-from-yesterday", stale); !errors.Is(err, recorder.ErrObjectChanged) {
		t.Fatalf("stale If-Match: err=%v, want ErrObjectChanged", err)
	}
	for _, p := range []string{stale, stale + ".part"} {
		if _, err := os.Stat(p); !os.IsNotExist(err) {
			t.Fatalf("%s exists after a failed download", p)
		}
	}

	// A missing object is an ordinary error: the caller re-reads the state.
	missing := filepath.Join(dir, "missing.m4a")
	err = s.Download(ctx, "2026-09-22/gone.m4a", "", missing)
	if err == nil || errors.Is(err, recorder.ErrObjectChanged) || isPermanent(err) {
		t.Fatalf("download of a missing object: %v", err)
	}
	if _, err := os.Stat(missing + ".part"); !os.IsNotExist(err) {
		t.Fatalf("partial file left behind for a missing object")
	}
	if keys := b.keys(); len(keys) != 1 || keys[0] != key {
		t.Fatalf("bucket holds %v", keys)
	}
}

func TestConditionalWritesProbe(t *testing.T) {
	ctx := context.Background()

	t.Run("enforcing endpoint", func(t *testing.T) {
		s, b := newTestS3(t)
		ok, err := s.ConditionalWrites(ctx)
		if err != nil || !ok {
			t.Fatalf("ConditionalWrites = %v, %v; want true", ok, err)
		}
		if keys := b.keys(); len(keys) != 0 {
			t.Fatalf("probe objects left behind: %v", keys)
		}
		if _, _, uploads := b.stats(); uploads != 0 {
			t.Fatalf("%d multipart uploads left behind", uploads)
		}
		// The answer is cached: no second probe, no second probe object.
		_, requests, _ := b.stats()
		ok, err = s.ConditionalWrites(ctx)
		if err != nil || !ok {
			t.Fatalf("cached ConditionalWrites = %v, %v", ok, err)
		}
		if _, again, _ := b.stats(); again != requests {
			t.Fatalf("cached call made %d extra requests", again-requests)
		}
	})

	t.Run("endpoint that ignores If-Match", func(t *testing.T) {
		s, b := newTestS3With(t, func(b *fakeBucket, _ *config.Config) { b.ignoreIfMatch = true })
		ok, err := s.ConditionalWrites(ctx)
		if err != nil || ok {
			t.Fatalf("ConditionalWrites = %v, %v; want false", ok, err)
		}
		if keys := b.keys(); len(keys) != 0 {
			t.Fatalf("probe objects left behind: %v", keys)
		}
	})

	t.Run("endpoint that enforces only on PutObject", func(t *testing.T) {
		s, b := newTestS3With(t, func(b *fakeBucket, _ *config.Config) { b.ignoreCompleteIfMatch = true })
		ok, err := s.ConditionalWrites(ctx)
		if err != nil || ok {
			t.Fatalf("ConditionalWrites = %v, %v; want false (multipart leg not enforced)", ok, err)
		}
		if keys := b.keys(); len(keys) != 0 {
			t.Fatalf("probe objects left behind: %v", keys)
		}
		if _, _, uploads := b.stats(); uploads != 0 {
			t.Fatalf("%d multipart uploads left behind", uploads)
		}
	})

	t.Run("conditional puts disabled", func(t *testing.T) {
		s, b := newTestS3With(t, func(_ *fakeBucket, cfg *config.Config) { cfg.S3ConditionalPut = false })
		ok, err := s.ConditionalWrites(ctx)
		if err != nil || ok {
			t.Fatalf("ConditionalWrites = %v, %v; want false", ok, err)
		}
		if _, requests, _ := b.stats(); requests != 0 {
			t.Fatalf("probed the endpoint %d times although conditional puts are off", requests)
		}
	})
}

// TestSanitizeMetadataReservedKeys pins the invariant the merge depends on: a
// long name or source must never cost the manifest that says which sessions
// the object already holds.
func TestSanitizeMetadataReservedKeys(t *testing.T) {
	digests := make([]string, 0, 30)
	for i := 0; i < 30; i++ {
		sum := sha256.Sum256([]byte(fmt.Sprintf("sess-%02d-20260922", i)))
		digests = append(digests, hex.EncodeToString(sum[:])[:8])
	}
	sessions := strings.Join(digests, ",") // 30*8 + 29 = 269 bytes
	reserved := map[string]string{
		"recording-id":       "match-ro-jpOkle8Mp0",
		"sessions":           sessions,
		"parts":              "30",
		"last-session-start": "2026-09-22T23:45:07Z",
	}
	in := map[string]string{
		"name":   strings.Repeat("A", 300),
		"source": "https://origin.example/live/" + strings.Repeat("segment/", 60) + "master.m3u8",
	}
	for k, v := range reserved {
		in[k] = v
	}
	// Enough other metadata to exhaust the 1800 byte budget several times over.
	// The keys sort after "name"/"source" so those two show the ordinary caps
	// rather than being the ones the budget happens to drop.
	for i := 0; i < 12; i++ {
		in[fmt.Sprintf("zfiller-%02d", i)] = strings.Repeat("f", 250)
	}

	got := SanitizeMetadata(in)
	for k, want := range reserved {
		if got[k] != want {
			t.Errorf("%s = %q, want the value verbatim (%q)", k, got[k], want)
		}
	}
	if len(got["sessions"]) != 269 {
		t.Errorf("sessions is %d bytes, want 269", len(got["sessions"]))
	}
	if len(got["name"]) != 256 {
		t.Errorf("name is %d bytes, want the ordinary 256 byte cap", len(got["name"]))
	}
	dropped := 0
	for i := 0; i < 12; i++ {
		if _, ok := got[fmt.Sprintf("zfiller-%02d", i)]; !ok {
			dropped++
		}
	}
	if dropped == 0 {
		t.Errorf("no filler entry was dropped: the size cap never applied, so the test proves nothing")
	}
	total := 0
	for k, v := range got {
		total += len(k) + len(v)
	}
	if total > 1800+len(sessions) {
		t.Errorf("total metadata size %d is larger than the budget plus the reserved manifest", total)
	}
}

func TestQuoteETag(t *testing.T) {
	cases := map[string]string{
		"abc":   `"abc"`,
		`"abc"`: `"abc"`,
		"":      `""`,
		`"`:     `"""`, // a lone quote is not a quoted ETag
	}
	for in, want := range cases {
		if got := quoteETag(in); got != want {
			t.Errorf("quoteETag(%q) = %q, want %q", in, got, want)
		}
	}
}
