package storage

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/stretchr/testify/require"
)

func TestS3AdapterImplementsDirectDownload(t *testing.T) {
	var _ Adapter = (*S3Adapter)(nil)
	var _ DirectDownloadAdapter = (*S3Adapter)(nil)
}

func TestS3KeyUsesCachePrefix(t *testing.T) {
	adapter := &S3Adapter{keyPrefix: s3KeyPrefix("custom-cache")}

	if got := adapter.key("/folder/object"); got != "custom-cache/folder/object" {
		t.Fatalf("unexpected key: %s", got)
	}
}

func TestS3KeyPrefixDefault(t *testing.T) {
	if got := s3KeyPrefix(""); got != config.DefaultS3KeyPrefix {
		t.Fatalf("unexpected key prefix: %s", got)
	}
}

func TestS3UploadOptionsDefault(t *testing.T) {
	require.Equal(t, int64(config.DefaultS3UploadPartSizeBytes), s3UploadPartSizeBytes(0))
	require.Equal(t, config.DefaultS3UploadConcurrency, s3UploadConcurrency(0))
	require.Equal(t, config.DefaultS3MultipartAbortTimeout, s3MultipartAbortTimeout(0))
}

func TestNewS3AdapterRejectsUploadPartSizeBelowMinimum(t *testing.T) {
	tests := []struct {
		partSize   int64
		configured bool
	}{
		{partSize: 0, configured: true},
		{partSize: config.MinS3UploadPartSizeBytes - 1},
	}

	for _, tt := range tests {
		adapter, err := NewS3Adapter(context.Background(), config.StorageConfig{
			S3Bucket:                        "cache-bucket",
			S3Region:                        "us-east-1",
			S3UploadPartSizeBytes:           tt.partSize,
			S3UploadPartSizeBytesConfigured: tt.configured,
		})
		require.Nil(t, adapter)
		require.Error(t, err)
		require.Contains(t, err.Error(), "STORAGE_S3_UPLOAD_PART_SIZE_BYTES")
		require.Contains(t, err.Error(), strconv.FormatInt(config.MinS3UploadPartSizeBytes, 10))
		require.Contains(t, err.Error(), strconv.FormatInt(tt.partSize, 10))
	}
}

func TestS3ClearUsesDelimitedPrefix(t *testing.T) {
	adapter := &S3Adapter{keyPrefix: s3KeyPrefix("gh-actions-cache")}

	if got := adapter.clearPrefix(); got != "gh-actions-cache/" {
		t.Fatalf("unexpected clear prefix: %s", got)
	}
}

func TestNewS3AdapterProbesConfiguredPrefix(t *testing.T) {
	fakeS3 := newFakeS3Server(t, fakeS3Options{})
	defer fakeS3.Close()

	_, err := newTestS3Adapter(t, fakeS3.URL)
	require.NoError(t, err)
	require.Equal(t, 0, fakeS3.headBucketCount())
	require.Equal(t, 1, fakeS3.listObjectsCount())
	require.Equal(t, "gh-actions-cache/", fakeS3.listObjectsPrefix())
	require.Equal(t, "1", fakeS3.listObjectsMaxKeys())
}

func TestNewS3AdapterDefaultsOmittedUploadPartSize(t *testing.T) {
	fakeS3 := newFakeS3Server(t, fakeS3Options{})
	defer fakeS3.Close()

	adapter, err := newTestS3AdapterWithConfig(t, config.StorageConfig{
		S3EndpointURL: fakeS3.URL,
	})
	require.NoError(t, err)
	require.Equal(t, int64(config.DefaultS3UploadPartSizeBytes), adapter.uploadPartSizeBytes)
}

func TestNewS3AdapterFailsWhenPrefixProbeCannotAccessBucket(t *testing.T) {
	fakeS3 := newFakeS3Server(t, fakeS3Options{listObjectsStatus: http.StatusNotFound})
	defer fakeS3.Close()

	_, err := newTestS3Adapter(t, fakeS3.URL)
	require.Error(t, err)
	require.Equal(t, 0, fakeS3.headBucketCount())
	require.Equal(t, 1, fakeS3.listObjectsCount())
}

func TestS3AdapterUploadStreamUsesMultipartForLargeObjects(t *testing.T) {
	ctx := context.Background()
	fakeS3 := newFakeS3Server(t, fakeS3Options{})
	defer fakeS3.Close()
	adapter, err := newTestS3Adapter(t, fakeS3.URL)
	require.NoError(t, err)

	body := bytes.NewReader(make([]byte, config.DefaultS3UploadPartSizeBytes+1))
	require.NoError(t, adapter.UploadStream(ctx, "folder/object", body))

	require.False(t, fakeS3.putObjectCalled())
	require.True(t, fakeS3.createMultipartCalled())
	require.Equal(t, []int{1, 2}, fakeS3.uploadedPartNumbers())
	require.Equal(t, []int{config.DefaultS3UploadPartSizeBytes, 1}, fakeS3.uploadedPartSizes())
	require.True(t, fakeS3.completeMultipartCalled())
	require.False(t, fakeS3.abortMultipartCalled())
}

func TestS3AdapterUploadStreamUsesConfiguredUploadOptions(t *testing.T) {
	ctx := context.Background()
	fakeS3 := newFakeS3Server(t, fakeS3Options{})
	defer fakeS3.Close()

	partSize := int64(config.DefaultS3UploadPartSizeBytes + 1024)
	adapter, err := newTestS3AdapterWithConfig(t, config.StorageConfig{
		S3EndpointURL:           fakeS3.URL,
		S3UploadPartSizeBytes:   partSize,
		S3UploadConcurrency:     2,
		S3MultipartAbortTimeout: 45 * time.Second,
	})
	require.NoError(t, err)
	require.Equal(t, partSize, adapter.uploadPartSizeBytes)
	require.Equal(t, 2, adapter.uploadConcurrency)
	require.Equal(t, 45*time.Second, adapter.multipartAbortTimeout)

	body := bytes.NewReader(make([]byte, int(partSize)+1))
	require.NoError(t, adapter.UploadStream(ctx, "folder/object", body))

	require.True(t, fakeS3.createMultipartCalled())
	require.Equal(t, map[int]int{1: int(partSize), 2: 1}, fakeS3.uploadedPartSizesByNumber())
}

func TestS3AdapterUploadStreamAbortsFailedMultipartUpload(t *testing.T) {
	requireFailedMultipartUploadAborted(t, context.Background(), fakeS3Options{failPartNumber: 2})
}

func TestS3AdapterUploadStreamAbortsCanceledMultipartUploadWithFreshContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	requireFailedMultipartUploadAborted(t, ctx, fakeS3Options{
		onUploadPart: func(partNumber int) {
			if partNumber == 1 {
				cancel()
			}
		},
	})
}

func requireFailedMultipartUploadAborted(t *testing.T, ctx context.Context, options fakeS3Options) {
	t.Helper()

	fakeS3 := newFakeS3Server(t, options)
	t.Cleanup(fakeS3.Close)
	adapter, err := newTestS3Adapter(t, fakeS3.URL)
	require.NoError(t, err)

	body := bytes.NewReader(make([]byte, config.DefaultS3UploadPartSizeBytes+1))
	err = adapter.UploadStream(ctx, "folder/object", body)
	require.Error(t, err)

	require.True(t, fakeS3.createMultipartCalled())
	require.True(t, fakeS3.abortMultipartCalled())
	require.False(t, fakeS3.completeMultipartCalled())
}

func TestS3AdapterCreateDownloadStreamTreatsMissingObjectAsNotFound(t *testing.T) {
	ctx := context.Background()
	fakeS3 := newFakeS3Server(t, fakeS3Options{
		getObjectStatus:    http.StatusNotFound,
		getObjectErrorCode: "NoSuchKey",
	})
	defer fakeS3.Close()
	adapter, err := newTestS3Adapter(t, fakeS3.URL)
	require.NoError(t, err)

	stream, err := adapter.CreateDownloadStream(ctx, "folder/object")
	require.Nil(t, stream)
	require.Error(t, err)
	var notFound ObjectNotFoundError
	require.True(t, errors.As(err, &notFound))
}

func TestS3AdapterCreateDownloadStreamDoesNotTreatMissingBucketAsObjectNotFound(t *testing.T) {
	ctx := context.Background()
	fakeS3 := newFakeS3Server(t, fakeS3Options{
		getObjectStatus:    http.StatusNotFound,
		getObjectErrorCode: "NoSuchBucket",
	})
	defer fakeS3.Close()
	adapter, err := newTestS3Adapter(t, fakeS3.URL)
	require.NoError(t, err)

	stream, err := adapter.CreateDownloadStream(ctx, "folder/object")
	require.Nil(t, stream)
	require.Error(t, err)
	var notFound ObjectNotFoundError
	require.False(t, errors.As(err, &notFound))
	require.Contains(t, err.Error(), "get s3 object")
}

func TestS3DeleteErrorsError(t *testing.T) {
	err := s3DeleteErrorsError([]types.Error{
		{
			Key:     aws.String("gh-actions-cache/folder/object"),
			Code:    aws.String("AccessDenied"),
			Message: aws.String("retention policy prevents deletion"),
		},
	})

	if err == nil {
		t.Fatal("expected per-object delete error")
	}
	for _, want := range []string{"gh-actions-cache/folder/object", "AccessDenied", "retention policy prevents deletion"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("expected error to contain %q, got %q", want, err.Error())
		}
	}
}

type fakeS3Options struct {
	headBucketStatus   int
	listObjectsStatus  int
	getObjectStatus    int
	getObjectErrorCode string
	failPartNumber     int
	onUploadPart       func(int)
}

type fakeS3Server struct {
	*httptest.Server
	headBucketStatus   int
	listObjectsStatus  int
	getObjectStatus    int
	getObjectErrorCode string
	failPartNumber     int
	onUploadPart       func(int)

	mu             sync.Mutex
	headBucket     int
	listObjects    int
	listPrefix     string
	listMaxKeys    string
	putObject      bool
	createUpload   bool
	uploadParts    []int
	uploadPartSize []int
	completeUpload bool
	abortUpload    bool
}

func newFakeS3Server(t *testing.T, options fakeS3Options) *fakeS3Server {
	t.Helper()

	if options.headBucketStatus == 0 {
		options.headBucketStatus = http.StatusOK
	}
	if options.listObjectsStatus == 0 {
		options.listObjectsStatus = http.StatusOK
	}
	if options.getObjectStatus == 0 {
		options.getObjectStatus = http.StatusOK
	}
	fakeS3 := &fakeS3Server{
		headBucketStatus:   options.headBucketStatus,
		listObjectsStatus:  options.listObjectsStatus,
		getObjectStatus:    options.getObjectStatus,
		getObjectErrorCode: options.getObjectErrorCode,
		failPartNumber:     options.failPartNumber,
		onUploadPart:       options.onUploadPart,
	}
	fakeS3.Server = httptest.NewServer(http.HandlerFunc(fakeS3.handle))
	return fakeS3
}

func (s *fakeS3Server) handle(w http.ResponseWriter, r *http.Request) {
	switch {
	case r.Method == http.MethodHead && r.URL.Path == "/cache-bucket":
		s.mu.Lock()
		s.headBucket++
		s.mu.Unlock()
		w.WriteHeader(s.headBucketStatus)
	case r.Method == http.MethodGet && r.URL.Path == "/cache-bucket" && r.URL.Query().Get("list-type") == "2":
		s.mu.Lock()
		s.listObjects++
		s.listPrefix = r.URL.Query().Get("prefix")
		s.listMaxKeys = r.URL.Query().Get("max-keys")
		status := s.listObjectsStatus
		s.mu.Unlock()
		if status != http.StatusOK {
			w.WriteHeader(status)
			_, _ = fmt.Fprint(w, `<Error><Code>NoSuchBucket</Code><Message>bucket not found</Message></Error>`)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = fmt.Fprint(w, `<ListBucketResult><Name>cache-bucket</Name><Prefix>gh-actions-cache/</Prefix><KeyCount>0</KeyCount><MaxKeys>1</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>`)
	case r.Method == http.MethodGet && strings.HasPrefix(r.URL.Path, "/cache-bucket/"):
		if s.getObjectStatus != http.StatusOK {
			code := s.getObjectErrorCode
			if code == "" {
				code = "NoSuchKey"
			}
			w.WriteHeader(s.getObjectStatus)
			_, _ = fmt.Fprintf(w, `<Error><Code>%s</Code><Message>get failed</Message></Error>`, code)
			return
		}
		_, _ = fmt.Fprint(w, "object")
	case r.Method == http.MethodPut && r.URL.Query().Get("partNumber") == "":
		_, _ = io.Copy(io.Discard, r.Body)
		s.mu.Lock()
		s.putObject = true
		s.mu.Unlock()
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodPost && hasQueryKey(r, "uploads"):
		s.mu.Lock()
		s.createUpload = true
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/xml")
		_, _ = fmt.Fprint(w, `<CreateMultipartUploadResult><Bucket>cache-bucket</Bucket><Key>gh-actions-cache/folder/object</Key><UploadId>upload-id</UploadId></CreateMultipartUploadResult>`)
	case r.Method == http.MethodPut && r.URL.Query().Get("uploadId") == "upload-id":
		partNumber, _ := strconv.Atoi(r.URL.Query().Get("partNumber"))
		size, _ := io.Copy(io.Discard, r.Body)
		s.mu.Lock()
		s.uploadParts = append(s.uploadParts, partNumber)
		s.uploadPartSize = append(s.uploadPartSize, int(size))
		onUploadPart := s.onUploadPart
		s.mu.Unlock()
		if onUploadPart != nil {
			onUploadPart(partNumber)
		}
		if partNumber == s.failPartNumber {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = fmt.Fprint(w, `<Error><Code>InternalError</Code><Message>part failed</Message></Error>`)
			return
		}
		w.Header().Set("ETag", fmt.Sprintf(`"part-%d"`, partNumber))
		w.WriteHeader(http.StatusOK)
	case r.Method == http.MethodPost && r.URL.Query().Get("uploadId") == "upload-id":
		_, _ = io.Copy(io.Discard, r.Body)
		s.mu.Lock()
		s.completeUpload = true
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/xml")
		_, _ = fmt.Fprint(w, `<CompleteMultipartUploadResult><Location>http://example/folder/object</Location><Bucket>cache-bucket</Bucket><Key>gh-actions-cache/folder/object</Key><ETag>"complete"</ETag></CompleteMultipartUploadResult>`)
	case r.Method == http.MethodDelete && r.URL.Query().Get("uploadId") == "upload-id":
		s.mu.Lock()
		s.abortUpload = true
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
	default:
		w.WriteHeader(http.StatusNotFound)
		_, _ = fmt.Fprintf(w, "unexpected request: %s %s", r.Method, r.URL.String())
	}
}

func (s *fakeS3Server) headBucketCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.headBucket
}

func (s *fakeS3Server) listObjectsCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listObjects
}

func (s *fakeS3Server) listObjectsPrefix() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listPrefix
}

func (s *fakeS3Server) listObjectsMaxKeys() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listMaxKeys
}

func (s *fakeS3Server) putObjectCalled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.putObject
}

func (s *fakeS3Server) createMultipartCalled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.createUpload
}

func (s *fakeS3Server) uploadedPartNumbers() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int(nil), s.uploadParts...)
}

func (s *fakeS3Server) uploadedPartSizes() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int(nil), s.uploadPartSize...)
}

func (s *fakeS3Server) uploadedPartSizesByNumber() map[int]int {
	s.mu.Lock()
	defer s.mu.Unlock()

	sizes := make(map[int]int, len(s.uploadParts))
	for index, partNumber := range s.uploadParts {
		sizes[partNumber] = s.uploadPartSize[index]
	}
	return sizes
}

func (s *fakeS3Server) completeMultipartCalled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.completeUpload
}

func (s *fakeS3Server) abortMultipartCalled() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.abortUpload
}

func hasQueryKey(r *http.Request, key string) bool {
	_, ok := r.URL.Query()[key]
	return ok
}

func newTestS3Adapter(t *testing.T, endpoint string) (*S3Adapter, error) {
	t.Helper()

	return newTestS3AdapterWithConfig(t, config.StorageConfig{S3EndpointURL: endpoint})
}

func newTestS3AdapterWithConfig(t *testing.T, cfg config.StorageConfig) (*S3Adapter, error) {
	t.Helper()

	t.Setenv("AWS_ACCESS_KEY_ID", "test")
	t.Setenv("AWS_SECRET_ACCESS_KEY", "test")
	t.Setenv("AWS_EC2_METADATA_DISABLED", "true")

	if cfg.S3Bucket == "" {
		cfg.S3Bucket = "cache-bucket"
	}
	if cfg.S3Region == "" {
		cfg.S3Region = "us-east-1"
	}
	if cfg.S3KeyPrefix == "" {
		cfg.S3KeyPrefix = config.DefaultS3KeyPrefix
	}
	return NewS3Adapter(context.Background(), config.StorageConfig{
		S3Bucket:                        cfg.S3Bucket,
		S3Region:                        cfg.S3Region,
		S3EndpointURL:                   cfg.S3EndpointURL,
		S3ForcePathStyle:                true,
		S3KeyPrefix:                     cfg.S3KeyPrefix,
		S3UploadPartSizeBytes:           cfg.S3UploadPartSizeBytes,
		S3UploadPartSizeBytesConfigured: cfg.S3UploadPartSizeBytesConfigured,
		S3UploadConcurrency:             cfg.S3UploadConcurrency,
		S3MultipartAbortTimeout:         cfg.S3MultipartAbortTimeout,
	})
}

func TestIsDirectS3FolderObject(t *testing.T) {
	tests := []struct {
		name   string
		prefix string
		key    string
		want   bool
	}{
		{
			name:   "direct object",
			prefix: "gh-actions-cache/folder/",
			key:    "gh-actions-cache/folder/a",
			want:   true,
		},
		{
			name:   "nested object",
			prefix: "gh-actions-cache/folder/",
			key:    "gh-actions-cache/folder/sub/b",
			want:   false,
		},
		{
			name:   "prefix marker",
			prefix: "gh-actions-cache/folder/",
			key:    "gh-actions-cache/folder/",
			want:   false,
		},
		{
			name:   "outside prefix",
			prefix: "gh-actions-cache/folder/",
			key:    "gh-actions-cache/other/a",
			want:   false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isDirectS3FolderObject(tt.prefix, tt.key); got != tt.want {
				t.Fatalf("unexpected direct object result: got %v want %v", got, tt.want)
			}
		})
	}
}
