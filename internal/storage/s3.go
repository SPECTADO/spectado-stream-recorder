// Package storage uploads finished recordings to an S3 compatible object
// store (Cloudflare R2).
package storage

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"os"
	"sort"
	"strings"
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
	conditional atomic.Bool
	checksum    types.ChecksumAlgorithm
	log         *slog.Logger
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
	s := &S3{client: client, uploader: up, bucket: cfg.S3Bucket, prefix: cfg.S3Prefix, log: log}
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
// It returns the object's ETag. Failures are classified: permanent-looking
// ones are wrapped in *recorder.PermanentError; a conditional-put conflict
// with a different object yields recorder.ErrObjectExists.
func (s *S3) Upload(ctx context.Context, path, key, contentType string, metadata map[string]string) (string, error) {
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
	conditional := s.conditional.Load()
	if conditional {
		in.IfNoneMatch = aws.String("*")
	}

	out, err := s.uploader.Upload(ctx, in)
	if err != nil {
		if ctx.Err() != nil {
			// The uploader aborts failed multipart uploads itself while the
			// context is live; only a cancelled context needs our fresh one.
			s.abortIfMultipart(err, key)
		}
		code, status := errorCodeAndStatus(err)
		switch {
		case conditional && (status == http.StatusPreconditionFailed || code == "PreconditionFailed"):
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

// Exists reports whether an object of exactly size bytes is stored under key.
func (s *S3) Exists(ctx context.Context, key string, size int64) (bool, error) {
	remote, err := s.headSize(ctx, key)
	if err != nil {
		code, status := errorCodeAndStatus(err)
		if status == http.StatusNotFound || code == "NotFound" || code == "NoSuchKey" {
			return false, nil
		}
		return false, err
	}
	return remote == size, nil
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
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	_, aerr := s.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket: aws.String(s.bucket), Key: aws.String(key), UploadId: aws.String(mf.UploadID()),
	})
	if aerr != nil {
		if code, _ := errorCodeAndStatus(aerr); code == "NoSuchUpload" {
			s.log.Debug("multipart upload already aborted", "key", key, "uploadId", mf.UploadID())
			return
		}
		s.log.Warn("abort multipart upload", "key", key, "uploadId", mf.UploadID(), "error", aerr)
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

// SanitizeMetadata makes user-provided metadata safe for S3 headers: keys are
// lower-cased and restricted to [a-z0-9-], values with non-ASCII characters
// are RFC 2047 encoded, and the total size stays under the 2 KiB limit.
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
	for _, orig := range keys {
		k := sanitizeKey(orig)
		v := sanitizeValue(in[orig], 256)
		if k == "" || v == "" {
			continue
		}
		if total+len(k)+len(v) > 1800 {
			continue
		}
		total += len(k) + len(v)
		out[k] = v
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
