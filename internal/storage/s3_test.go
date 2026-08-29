package storage

import (
	"context"
	"errors"
	"fmt"
	"mime"
	"net"
	"net/http"
	"strings"
	"testing"
	"unicode/utf8"

	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	"github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

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
