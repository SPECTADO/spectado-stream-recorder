// Package storage uploads finished recordings to an S3 compatible object
// store (Cloudflare R2).
package storage

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/aws/ratelimit"
	"github.com/aws/aws-sdk-go-v2/aws/retry"
	awshttp "github.com/aws/aws-sdk-go-v2/aws/transport/http"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/feature/s3/manager"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"

	"github.com/spectado/stream-recorder/internal/config"
	"github.com/spectado/stream-recorder/internal/recorder"
)

// S3 uploads files to a bucket.
type S3 struct {
	client      *s3.Client
	uploader    *manager.Uploader
	bucket      string
	prefix      string
	partSize    int64 // also the download chunk size
	conditional atomic.Bool
	checksum    types.ChecksumAlgorithm
	log         *slog.Logger

	// The conditional-write probe runs at most once per process; the mutex
	// keeps two upload workers from probing (and writing a probe object) at
	// the same time.
	probeMu     sync.Mutex
	probed      bool
	probeResult bool
}

// NewS3 builds the client from configuration.
func NewS3(ctx context.Context, cfg *config.Config, log *slog.Logger) (*S3, error) {
	creds := credentials.NewStaticCredentialsProvider(cfg.S3AccessKeyID, cfg.S3SecretAccessKey, "")
	httpClient := awshttp.NewBuildableClient().WithTransportOptions(func(t *http.Transport) {
		t.MaxIdleConnsPerHost = 32
		t.ResponseHeaderTimeout = 60 * time.Second
		t.IdleConnTimeout = 90 * time.Second
	})
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion(cfg.S3Region),
		awsconfig.WithCredentialsProvider(creds),
		awsconfig.WithHTTPClient(httpClient),
		// R2 does not accept the CRC checksums the SDK adds by default to
		// every request; only compute them when an operation requires it.
		awsconfig.WithRequestChecksumCalculation(aws.RequestChecksumCalculationWhenRequired),
		awsconfig.WithResponseChecksumValidation(aws.ResponseChecksumValidationWhenRequired),
		// The recorder has its own infinite retry loop; disable the SDK's
		// retry-quota so an outage does not surface as "quota exceeded".
		awsconfig.WithRetryer(func() aws.Retryer {
			return retry.NewStandard(func(o *retry.StandardOptions) {
				o.MaxAttempts = 4
				o.MaxBackoff = 20 * time.Second
				o.RateLimiter = ratelimit.None
			})
		}),
	)
	if err != nil {
		return nil, fmt.Errorf("aws config: %w", err)
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.S3Endpoint)
		o.UsePathStyle = cfg.S3ForcePathStyle
	})
	up := manager.NewUploader(client, func(u *manager.Uploader) {
		u.PartSize = cfg.UploadPartSize
		u.Concurrency = 3
		u.LeavePartsOnError = false
		// The uploader has its own copy of this setting (independent of the
		// client); without it every multipart upload still carries a CRC32 trailer.
		u.RequestChecksumCalculation = aws.RequestChecksumCalculationWhenRequired
	})
	s := &S3{
		client: client, uploader: up, bucket: cfg.S3Bucket, prefix: cfg.S3Prefix,
		partSize: cfg.UploadPartSize, log: log,
	}
	s.conditional.Store(cfg.S3ConditionalPut)
	switch cfg.S3ChecksumAlgorithm {
	case "crc32":
		s.checksum = types.ChecksumAlgorithmCrc32
	case "crc32c":
		s.checksum = types.ChecksumAlgorithmCrc32c
	}
	return s, nil
}

// Check verifies that the bucket is reachable with the configured credentials.
func (s *S3) Check(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	_, err := s.client.HeadBucket(ctx, &s3.HeadBucketInput{Bucket: aws.String(s.bucket)})
	return err
}

// Upload stores the file under key, then verifies the stored object's size.
// It returns the object's ETag without the surrounding quotes. Failures are
// classified: permanent-looking ones are wrapped in *recorder.PermanentError.
//
// With an empty replaceETag the object must not already exist with different
// content: the PUT carries If-None-Match: * (when S3_CONDITIONAL_PUT is on)
// and a conflict with a differently sized object yields
// recorder.ErrObjectExists. With a replaceETag the upload replaces exactly the
// object carrying that ETag (If-Match) — this is how a later session of a
// recording is merged into the object that already holds the earlier ones, so
// a 412 must surface as recorder.ErrObjectChanged (re-read and merge again)
// and an endpoint that cannot do If-Match must never fall back to an
// unconditional PUT: that would drop whatever the object already held.
func (s *S3) Upload(ctx context.Context, path, key, contentType string, metadata map[string]string, replaceETag string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	st, err := f.Stat()
	if err != nil {
		return "", err
	}
	size := st.Size()
	if size == 0 {
		return "", &recorder.PermanentError{Err: errors.New("refusing to upload an empty file")}
	}

	in := &s3.PutObjectInput{
		Bucket:        aws.String(s.bucket),
		Key:           aws.String(key),
		Body:          f, // *os.File: the uploader reads parts with ReadAt, no buffering
		ContentType:   aws.String(contentType),
		ContentLength: aws.Int64(size),
		Metadata:      SanitizeMetadata(metadata),
	}
	if s.checksum != "" {
		in.ChecksumAlgorithm = s.checksum
	}
	replacing := replaceETag != ""
	conditional := false
	if replacing {
		// The SDK copies same-named fields from the PutObjectInput into the
		// CompleteMultipartUploadInput, so this condition holds on both the
		// single-part and the multipart path.
		in.IfMatch = aws.String(quoteETag(replaceETag))
	} else {
		conditional = s.conditional.Load()
		if conditional {
			in.IfNoneMatch = aws.String("*")
		}
	}

	out, err := s.uploader.Upload(ctx, in)
	if err != nil {
		if ctx.Err() != nil {
			// The uploader aborts failed multipart uploads itself while the
			// context is live; only a cancelled context needs our fresh one.
			s.abortIfMultipart(err, key)
		}
		code, status := errorCodeAndStatus(err)
		precondition := status == http.StatusPreconditionFailed || code == "PreconditionFailed"
		switch {
		case replacing && precondition:
			// Someone else wrote the object between our HEAD and this PUT.
			return "", recorder.ErrObjectChanged
		case replacing && code == "NotImplemented":
			// Retrying without the condition would overwrite the other
			// writer's audio; the merge stays blocked instead.
			return "", &recorder.PermanentError{
				Err: fmt.Errorf("endpoint does not support If-Match on %s: %w", key, err),
			}
		case replacing:
			return "", classify(err)
		case conditional && precondition:
			// Something already lives under this key. Idempotent recovery: if it
			// has exactly our size, treat it as our own earlier upload.
			remote, herr := s.headSize(ctx, key)
			if herr != nil {
				return "", fmt.Errorf("conditional put conflict on %s but verification failed: %w", key, herr)
			}
			if remote == size {
				s.log.Info("object already present with matching size; treating as uploaded", "key", key)
				return "", nil
			}
			return "", recorder.ErrObjectExists
		case conditional && code == "NotImplemented" && mentionsConditional(err):
			// The endpoint does not support conditional writes; retry without.
			s.log.Warn("endpoint does not support If-None-Match; disabling conditional puts", "error", err)
			s.conditional.Store(false)
			if _, serr := f.Seek(0, 0); serr != nil {
				return "", serr
			}
			in.IfNoneMatch = nil
			out, err = s.uploader.Upload(ctx, in)
			if err != nil {
				if ctx.Err() != nil {
					s.abortIfMultipart(err, key)
				}
				return "", classify(err)
			}
		default:
			return "", classify(err)
		}
	}

	// Confirm the object landed with the expected size before the caller
	// deletes the local copy.
	remote, err := s.headSize(ctx, key)
	if err != nil {
		return "", classify(fmt.Errorf("verify upload (head): %w", err))
	}
	if remote != size {
		return "", fmt.Errorf("verify upload: remote size %d != local size %d", remote, size)
	}
	etag := ""
	if out != nil {
		etag = strings.Trim(aws.ToString(out.ETag), `"`)
	}
	return etag, nil
}

// Head reports size, ETag and user metadata of the object under key. found is
// false when there is no such object; everything else is classified. The
// upload path reads the manifest (recording-id, sessions, parts) from the
// metadata to decide whether the object is ours and which sessions it holds.
func (s *S3) Head(ctx context.Context, key string) (recorder.ObjectInfo, bool, error) {
	hctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	out, err := s.client.HeadObject(hctx, &s3.HeadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		if isNotFound(err) {
			return recorder.ObjectInfo{}, false, nil
		}
		return recorder.ObjectInfo{}, false, classify(err)
	}
	info := recorder.ObjectInfo{
		Size: aws.ToInt64(out.ContentLength),
		ETag: strings.Trim(aws.ToString(out.ETag), `"`),
	}
	if len(out.Metadata) > 0 {
		// The SDK already lower-cases the x-amz-meta-* names; copy so the
		// caller owns the map.
		info.Metadata = make(map[string]string, len(out.Metadata))
		for k, v := range out.Metadata {
			info.Metadata[strings.ToLower(k)] = v
		}
	}
	return info, true, nil
}

// Download writes the object at key to path using ranged GETs. It fails with
// recorder.ErrObjectChanged when ifMatchETag is given and the object is no
// longer the one that ETag describes — the merge is built on the manifest read
// by an earlier HEAD, so a different object must not be mixed into it.
//
// The bytes land in path+".part" and are renamed only after the size has been
// confirmed and the file is on disk: a truncated download would still parse as
// an MP4 and would silently cost the audio it is missing.
func (s *S3) Download(ctx context.Context, key, ifMatchETag, path string) error {
	tmp := path + ".part"
	f, err := os.Create(tmp)
	if err != nil {
		return err
	}
	closed := false
	defer func() {
		if !closed {
			f.Close()
			os.Remove(tmp)
		}
	}()

	in := &s3.GetObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)}
	if ifMatchETag != "" {
		in.IfMatch = aws.String(quoteETag(ifMatchETag))
	}
	d := manager.NewDownloader(s.client, func(d *manager.Downloader) {
		d.PartSize = s.partSize
		d.Concurrency = 3
	})
	n, err := d.Download(ctx, f, in)
	if err != nil {
		code, status := errorCodeAndStatus(err)
		if ifMatchETag != "" && (status == http.StatusPreconditionFailed || code == "PreconditionFailed") {
			return recorder.ErrObjectChanged
		}
		return classify(fmt.Errorf("download %s: %w", key, err))
	}
	remote, err := s.headSize(ctx, key)
	if err != nil {
		return classify(fmt.Errorf("verify download of %s (head): %w", key, err))
	}
	if remote != n {
		return fmt.Errorf("download %s: wrote %d bytes, object has %d", key, n, remote)
	}
	if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close %s: %w", tmp, err)
	}
	closed = true
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return err
	}
	return nil
}

// ConditionalWrites reports whether the endpoint really enforces
// If-None-Match/If-Match, on the single-part PUT *and* on
// CompleteMultipartUpload (the SDK's uploader picks the path by size, so a
// recording can take either). An endpoint that ignores the header answers 200
// and would let one writer silently discard another's audio, so merging
// sessions into one object is only allowed when this returns true. The answer
// is probed once with a throw-away object and cached for the process.
func (s *S3) ConditionalWrites(ctx context.Context) (bool, error) {
	s.probeMu.Lock()
	defer s.probeMu.Unlock()
	if s.probed {
		return s.probeResult, nil
	}
	if !s.conditional.Load() {
		// S3_CONDITIONAL_PUT=false (or an endpoint that answered
		// NotImplemented earlier): nothing sends a precondition, so nothing
		// enforces one. No need to write a probe object.
		s.probed, s.probeResult = true, false
		return false, nil
	}
	ok, err := s.probeConditionalWrites(ctx)
	if err != nil {
		// A store that is simply unreachable says nothing about its
		// preconditions; leave it unprobed so the next attempt tries again.
		return false, err
	}
	s.probed, s.probeResult = true, ok
	return ok, nil
}

// probeConditionalWrites writes a tiny object under `{prefix}.recorder-probe/`
// and checks that every conditional write against it is refused with 412.
func (s *S3) probeConditionalWrites(ctx context.Context) (bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()

	suffix := make([]byte, 8)
	if _, err := rand.Read(suffix); err != nil {
		return false, fmt.Errorf("conditional write probe key: %w", err)
	}
	key := s.prefix + ".recorder-probe/" + hex.EncodeToString(suffix)
	body := []byte("spectado-stream-recorder conditional write probe\n")
	put := func(condition func(*s3.PutObjectInput)) error {
		in := &s3.PutObjectInput{
			Bucket:        aws.String(s.bucket),
			Key:           aws.String(key),
			Body:          bytes.NewReader(body),
			ContentLength: aws.Int64(int64(len(body))),
			ContentType:   aws.String("text/plain"),
		}
		if condition != nil {
			condition(in)
		}
		_, err := s.client.PutObject(ctx, in)
		return err
	}

	if err := put(nil); err != nil {
		return false, classify(fmt.Errorf("conditional write probe (put %s): %w", key, err))
	}
	defer s.deleteProbe(key)

	if err := put(func(in *s3.PutObjectInput) { in.IfNoneMatch = aws.String("*") }); !isPreconditionFailed(err) {
		s.log.Warn("endpoint does not enforce If-None-Match on PutObject", "key", key, "error", err)
		return false, nil
	}
	if err := put(func(in *s3.PutObjectInput) { in.IfMatch = aws.String(`"deadbeef"`) }); !isPreconditionFailed(err) {
		s.log.Warn("endpoint does not enforce If-Match on PutObject", "key", key, "error", err)
		return false, nil
	}
	return s.probeMultipartIfMatch(ctx, key, body)
}

// probeMultipartIfMatch completes a one-part multipart upload with a wrong
// If-Match: the condition travels on CompleteMultipartUpload, which is a
// different code path in every implementation, so it is probed separately.
func (s *S3) probeMultipartIfMatch(ctx context.Context, key string, body []byte) (bool, error) {
	create, err := s.client.CreateMultipartUpload(ctx, &s3.CreateMultipartUploadInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key),
	})
	if err != nil {
		s.log.Warn("conditional write probe: create multipart upload failed", "key", key, "error", err)
		return false, nil
	}
	uploadID := aws.ToString(create.UploadId)
	// The complete below is meant to fail, so the upload always needs aborting;
	// leftover parts are billable.
	defer s.abortUpload(key, uploadID)

	part, err := s.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key), UploadId: aws.String(uploadID),
		PartNumber: aws.Int32(1), Body: bytes.NewReader(body), ContentLength: aws.Int64(int64(len(body))),
	})
	if err != nil {
		s.log.Warn("conditional write probe: upload part failed", "key", key, "error", err)
		return false, nil
	}
	_, err = s.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key), UploadId: aws.String(uploadID),
		MultipartUpload: &types.CompletedMultipartUpload{
			Parts: []types.CompletedPart{{ETag: part.ETag, PartNumber: aws.Int32(1)}},
		},
		IfMatch: aws.String(`"deadbeef"`),
	})
	if !isPreconditionFailed(err) {
		s.log.Warn("endpoint does not enforce If-Match on CompleteMultipartUpload", "key", key, "error", err)
		return false, nil
	}
	return true, nil
}

// deleteProbe removes the probe object with a fresh context: the probe runs
// inside an upload attempt whose context may already be on its way out.
func (s *S3) deleteProbe(key string) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if _, err := s.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key),
	}); err != nil {
		s.log.Warn("delete conditional write probe object", "key", key, "error", err)
	}
}

// isPreconditionFailed reports whether err is the 412 a refused conditional
// write produces. A nil error is not one: the write went through.
func isPreconditionFailed(err error) bool {
	if err == nil {
		return false
	}
	code, status := errorCodeAndStatus(err)
	return status == http.StatusPreconditionFailed || code == "PreconditionFailed"
}

// isNotFound reports whether err says the object does not exist (HEAD answers
// 404 with no code, GET with NoSuchKey).
func isNotFound(err error) bool {
	code, status := errorCodeAndStatus(err)
	return status == http.StatusNotFound || code == "NotFound" || code == "NoSuchKey"
}

// quoteETag returns the ETag in the quoted form the If-Match/If-None-Match
// headers require; ObjectInfo carries it unquoted.
func quoteETag(etag string) string {
	if strings.HasPrefix(etag, `"`) && strings.HasSuffix(etag, `"`) && len(etag) > 1 {
		return etag
	}
	return `"` + etag + `"`
}

// mentionsConditional reports whether an error message refers to conditional
// writes (If-None-Match), so a NotImplemented for another feature is not
// blamed on them.
func mentionsConditional(err error) bool {
	var ae smithy.APIError
	msg := err.Error()
	if errors.As(err, &ae) {
		msg += " " + ae.ErrorMessage()
	}
	msg = strings.ToLower(msg)
	return strings.Contains(msg, "if-none-match") || strings.Contains(msg, "conditional")
}

func (s *S3) headSize(ctx context.Context, key string) (int64, error) {
	hctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	head, err := s.client.HeadObject(hctx, &s3.HeadObjectInput{Bucket: aws.String(s.bucket), Key: aws.String(key)})
	if err != nil {
		return 0, err
	}
	return aws.ToInt64(head.ContentLength), nil
}

// abortIfMultipart aborts an interrupted multipart upload with a fresh
// context (the uploader's own abort uses the possibly-cancelled request
// context and fails silently), so no billable parts linger in the bucket.
func (s *S3) abortIfMultipart(err error, key string) {
	var mf manager.MultiUploadFailure
	if !errors.As(err, &mf) || mf.UploadID() == "" {
		return
	}
	s.abortUpload(key, mf.UploadID())
}

// abortUpload drops an unfinished multipart upload, with its own context for
// the same reason as above.
func (s *S3) abortUpload(key, uploadID string) {
	if uploadID == "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, err := s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key), UploadId: aws.String(uploadID),
	})
	if err != nil {
		if code, _ := errorCodeAndStatus(err); code == "NoSuchUpload" {
			s.log.Debug("multipart upload already aborted", "key", key, "uploadId", uploadID)
			return
		}
		s.log.Warn("abort multipart upload", "key", key, "uploadId", uploadID, "error", err)
	}
}

// CleanupStaleMultipartUploads aborts multipart uploads under the configured
// prefix that were started more than olderThan ago. Called at startup, when
// nothing can legitimately be in flight. Returns the number aborted.
func (s *S3) CleanupStaleMultipartUploads(ctx context.Context, olderThan time.Duration) (int, error) {
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	var keyMarker, uploadIDMarker *string
	aborted := 0
	for {
		in := &s3.ListMultipartUploadsInput{Bucket: aws.String(s.bucket), KeyMarker: keyMarker, UploadIdMarker: uploadIDMarker}
		if s.prefix != "" {
			in.Prefix = aws.String(s.prefix)
		}
		out, err := s.client.ListMultipartUploads(ctx, in)
		if err != nil {
			return aborted, err
		}
		for _, u := range out.Uploads {
			if u.Initiated == nil || time.Since(*u.Initiated) < olderThan {
				continue
			}
			_, err := s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
				Bucket: aws.String(s.bucket), Key: u.Key, UploadId: u.UploadId,
			})
			if err != nil {
				s.log.Warn("abort stale multipart upload", "key", aws.ToString(u.Key), "error", err)
				continue
			}
			aborted++
		}
		if !aws.ToBool(out.IsTruncated) || out.NextKeyMarker == nil {
			return aborted, nil
		}
		keyMarker, uploadIDMarker = out.NextKeyMarker, out.NextUploadIdMarker
	}
}

// errorCodeAndStatus extracts the S3 error code and HTTP status, if any.
func errorCodeAndStatus(err error) (code string, status int) {
	var ae smithy.APIError
	if errors.As(err, &ae) {
		code = ae.ErrorCode()
	}
	var re *awshttp.ResponseError
	if errors.As(err, &re) {
		status = re.HTTPStatusCode()
	}
	return code, status
}

// classify wraps failures that retrying quickly will not fix.
func classify(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	var nerr net.Error
	if errors.As(err, &nerr) {
		return err // network trouble: transient
	}
	code, status := errorCodeAndStatus(err)
	switch code {
	// Transient conditions that happen to use 4xx statuses.
	case "RequestTimeout", "RequestTimeoutException", "IncompleteBody", "XAmzContentSHA256Mismatch", "BadDigest",
		"NoSuchUpload", "InternalError", "SlowDown", "ServiceUnavailable", "Throttling", "ThrottlingException",
		"RequestLimitExceeded", "OperationAborted", "PreconditionFailed", "TooManyRequests":
		return err
	case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch", "NoSuchBucket", "InvalidBucketName",
		"MetadataTooLarge", "InvalidArgument", "EntityTooLarge", "KeyTooLongError", "InvalidRequest",
		"Unauthorized", "Forbidden", "AuthorizationHeaderMalformed", "AllAccessDisabled", "AccountProblem":
		return &recorder.PermanentError{Err: err}
	}
	switch status {
	case http.StatusUnauthorized, http.StatusForbidden:
		return &recorder.PermanentError{Err: err}
	}
	return err
}

// reservedMetadata are the keys the upload path reads back from the stored
// object to decide whether it may merge a new session into it. Losing one of
// them turns the object into a foreign object and costs a second key, so they
// are written first, get a larger per-value budget and are never dropped.
var reservedMetadata = map[string]bool{
	"recording-id":       true,
	"sessions":           true,
	"parts":              true,
	"last-session-start": true,
}

// reservedValueMax is the per-value cap for the merge manifest: the sessions
// list holds up to 20 x 9 bytes, well inside it, while a name or source is
// still cut at 256.
const reservedValueMax = 1024

// SanitizeMetadata makes user-provided metadata safe for S3 headers: keys are
// lower-cased and restricted to [a-z0-9-], values with non-ASCII characters
// are RFC 2047 encoded, and the total size stays under the 2 KiB limit.
// The reserved merge-manifest keys are processed first so a long title can
// never push them out.
func SanitizeMetadata(in map[string]string) map[string]string {
	if len(in) == 0 {
		return nil
	}
	keys := make([]string, 0, len(in))
	for k := range in {
		keys = append(keys, k)
	}
	sort.Strings(keys) // deterministic selection when the size cap applies
	out := make(map[string]string, len(in))
	total := 0
	for _, pass := range []bool{true, false} {
		for _, orig := range keys {
			k := sanitizeKey(orig)
			if k == "" || reservedMetadata[k] != pass {
				continue
			}
			if _, taken := out[k]; taken {
				continue // two inputs sanitize to the same key
			}
			max := 256
			if pass {
				max = reservedValueMax
			}
			v := sanitizeValue(in[orig], max)
			if v == "" {
				continue
			}
			// Reserved values are kept whatever the budget says; they are what
			// makes the object mergeable at all.
			if !pass && total+len(k)+len(v) > 1800 {
				continue
			}
			total += len(k) + len(v)
			out[k] = v
		}
	}
	return out
}

func sanitizeKey(k string) string {
	var sb strings.Builder
	for _, r := range strings.ToLower(k) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
			sb.WriteRune(r)
		case r == '_' || r == ' ' || r == '.':
			sb.WriteByte('-')
		}
	}
	return strings.Trim(sb.String(), "-")
}

// sanitizeValue strips control characters, truncates to max bytes and RFC
// 2047 encodes values containing non-ASCII characters (R2/S3 headers must be
// US-ASCII).
func sanitizeValue(v string, max int) string {
	var sb strings.Builder
	ascii := true
	for _, r := range v {
		switch {
		case r < 0x20 || r == 0x7f:
			continue
		case r > 0x7e:
			ascii = false
		}
		sb.WriteRune(r)
	}
	s := strings.TrimSpace(sb.String())
	if len(s) > max {
		s = truncateUTF8(s, max)
	}
	if !ascii {
		// The encoded form is ~1.4x larger plus wrappers; shrink the source
		// until the encoded value stays within 2*max bytes.
		plain := s
		s = mime.BEncoding.Encode("utf-8", plain)
		for len(s) > 2*max && len(plain) > 8 {
			plain = truncateUTF8(plain, len(plain)*3/4)
			s = mime.BEncoding.Encode("utf-8", plain)
		}
	}
	return s
}

func truncateUTF8(s string, max int) string {
	if len(s) <= max {
		return s
	}
	cut := max
	for cut > 0 && !isRuneStart(s[cut]) {
		cut--
	}
	return s[:cut]
}

func isRuneStart(b byte) bool { return b&0xC0 != 0x80 }
