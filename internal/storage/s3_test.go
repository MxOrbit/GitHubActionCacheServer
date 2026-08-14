package storage

import (
	"bytes"
	"context"
	"encoding/xml"
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
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/require"
)

// Two sources require 2 HEADs, 1 multipart create, 2 part copies, and 1 multipart complete.
const s3TwoSourceComposeRequestCount = 6

func TestS3AdapterImplementsDirectDownload(t *testing.T) {
	var _ Adapter = (*S3Adapter)(nil)
	var _ DirectDownloadAdapter = (*S3Adapter)(nil)
	var _ ComposeAdapter = (*S3Adapter)(nil)
}

func TestIsS3ComposeUnsupported(t *testing.T) {
	tests := []struct {
		code string
		want bool
	}{
		{code: "EntityTooSmall", want: true},
		{code: "NotImplemented", want: true},
		{code: "NotImplementedException", want: true},
		{code: "InvalidArgument", want: false},
		{code: "InvalidRequest", want: false},
		{code: "SlowDown", want: false},
	}

	for _, test := range tests {
		t.Run(test.code, func(t *testing.T) {
			err := &smithy.GenericAPIError{Code: test.code, Message: "test error"}
			require.Equal(t, test.want, isS3ComposeUnsupported(err))
		})
	}

	require.True(t, isS3ComposeUnsupported(fmt.Errorf("compose plan: %w", ErrComposeUnsupported)))
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

func TestS3AdapterCreatesAndValidatesExactRangedStream(t *testing.T) {
	var receivedRange string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodHead:
			w.WriteHeader(http.StatusOK)
		case r.Method == http.MethodGet && r.URL.Path == "/cache-bucket" && r.URL.Query().Get("list-type") == "2":
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprint(w, `<ListBucketResult><Name>cache-bucket</Name><KeyCount>0</KeyCount><MaxKeys>1</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>`)
		case r.Method == http.MethodGet && r.URL.Path == "/cache-bucket/gh-actions-cache/folder/object":
			receivedRange = r.Header.Get("Range")
			w.Header().Set("Content-Length", "3")
			w.Header().Set("Content-Range", "bytes 2-4/6")
			w.WriteHeader(http.StatusPartialContent)
			_, _ = fmt.Fprint(w, "jec")
		default:
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer server.Close()
	adapter, err := newTestS3Adapter(t, server.URL)
	require.NoError(t, err)

	stream, err := adapter.CreateRangedDownloadStream(context.Background(), "folder/object", 2, 3)
	require.NoError(t, err)
	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.Equal(t, "jec", string(body))
	require.Equal(t, "bytes=2-4", receivedRange)
	require.NoError(t, stream.Close())
}

func TestS3AdapterRejectsBackendThatIgnoresRange(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodHead {
			w.WriteHeader(http.StatusOK)
			return
		}
		if r.Method == http.MethodGet && r.URL.Path == "/cache-bucket" && r.URL.Query().Get("list-type") == "2" {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprint(w, `<ListBucketResult><Name>cache-bucket</Name><KeyCount>0</KeyCount><MaxKeys>1</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>`)
			return
		}
		w.Header().Set("Content-Length", "6")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, "object")
	}))
	defer server.Close()
	adapter, err := newTestS3Adapter(t, server.URL)
	require.NoError(t, err)

	stream, err := adapter.CreateRangedDownloadStream(context.Background(), "folder/object", 2, 3)
	require.Nil(t, stream)
	require.ErrorContains(t, err, "does not match requested count")
}

func TestS3AdapterRejectsNonzeroRangeWithoutContentRange(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Path == "/cache-bucket" && r.URL.Query().Get("list-type") == "2" {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprint(w, `<ListBucketResult><Name>cache-bucket</Name><KeyCount>0</KeyCount><MaxKeys>1</MaxKeys><IsTruncated>false</IsTruncated></ListBucketResult>`)
			return
		}
		w.Header().Set("Content-Length", "3")
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprint(w, "obj")
	}))
	defer server.Close()
	adapter, err := newTestS3Adapter(t, server.URL)
	require.NoError(t, err)

	stream, err := adapter.CreateRangedDownloadStream(context.Background(), "folder/object", 2, 3)
	require.Nil(t, stream)
	require.ErrorContains(t, err, "invalid content range")
}

func TestS3AdapterObjectExistsUsesHeadAndClassifiesErrors(t *testing.T) {
	ctx := context.Background()
	t.Run("exists", func(t *testing.T) {
		fakeS3 := newFakeS3Server(t, fakeS3Options{objectSizes: map[string]int64{
			"/cache-bucket/gh-actions-cache/folder/object": 4,
		}})
		defer fakeS3.Close()
		adapter, err := newTestS3Adapter(t, fakeS3.URL)
		require.NoError(t, err)

		exists, err := adapter.ObjectExists(ctx, "folder/object")
		require.NoError(t, err)
		require.True(t, exists)
		require.Equal(t, []string{"/cache-bucket/gh-actions-cache/folder/object"}, fakeS3.headObjectPaths())
	})

	t.Run("missing object", func(t *testing.T) {
		fakeS3 := newFakeS3Server(t, fakeS3Options{})
		defer fakeS3.Close()
		adapter, err := newTestS3Adapter(t, fakeS3.URL)
		require.NoError(t, err)

		exists, err := adapter.ObjectExists(ctx, "folder/missing")
		require.NoError(t, err)
		require.False(t, exists)
	})

	for _, tt := range []struct {
		name   string
		status int
		code   string
	}{
		{name: "access denied", status: http.StatusForbidden, code: "AccessDenied"},
		{name: "server failure", status: http.StatusInternalServerError, code: "InternalError"},
	} {
		t.Run(tt.name, func(t *testing.T) {
			fakeS3 := newFakeS3Server(t, fakeS3Options{
				headObjectStatus:    tt.status,
				headObjectErrorCode: tt.code,
			})
			defer fakeS3.Close()
			adapter, err := newTestS3Adapter(t, fakeS3.URL)
			require.NoError(t, err)

			exists, err := adapter.ObjectExists(ctx, "folder/object")
			require.Error(t, err)
			require.False(t, exists)
		})
	}
}

func TestS3SharedListingRejectsTruncatedPageWithoutToken(t *testing.T) {
	consumerListCalls := 0
	deleteCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
			w.Header().Set("Content-Type", "application/xml")
			if r.URL.Query().Get("max-keys") == "1" {
				_, _ = fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
				return
			}
			consumerListCalls++
			key := r.URL.Query().Get("prefix") + "0"
			_, _ = fmt.Fprintf(w, `<ListBucketResult><IsTruncated>true</IsTruncated><Contents><Key>%s</Key><LastModified>2026-07-29T00:00:00Z</LastModified><Size>1</Size></Contents></ListBucketResult>`, key)
			return
		}
		if r.Method == http.MethodPost && hasQueryKey(r, "delete") {
			deleteCalls++
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	adapter, err := newTestS3Adapter(t, server.URL)
	require.NoError(t, err)

	_, err = adapter.InspectFolderSummary(context.Background(), "folder")
	require.ErrorContains(t, err, "truncated response has no continuation token")
	_, err = adapter.CountFilesInFolder(context.Background(), "folder/parts")
	require.ErrorContains(t, err, "truncated response has no continuation token")
	require.Equal(t, 2, consumerListCalls)
	require.Zero(t, deleteCalls)
}

func TestS3SharedListingPaginatesInspectionAndCounting(t *testing.T) {
	continuationRequests := 0
	const basePrefix = "gh-actions-cache/"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
			w.Header().Set("Content-Type", "application/xml")
			if r.URL.Query().Get("max-keys") == "1" {
				_, _ = fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
				return
			}
			prefix := r.URL.Query().Get("prefix")
			delimiter := r.URL.Query().Get("delimiter")
			if prefix == basePrefix && delimiter == "/" {
				_, _ = fmt.Fprintf(w, `<ListBucketResult><IsTruncated>false</IsTruncated><CommonPrefixes><Prefix>%sfolder/</Prefix></CommonPrefixes></ListBucketResult>`, basePrefix)
				return
			}
			if !validPaginationTestPrefix(prefix, delimiter, basePrefix) {
				http.Error(w, "unexpected listing scope", http.StatusBadRequest)
				return
			}
			if r.URL.Query().Get("continuation-token") == "next" {
				continuationRequests++
				_, _ = fmt.Fprintf(w, `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>%sfolder/parts/1</Key><LastModified>2026-07-29T00:01:00Z</LastModified><Size>4</Size></Contents></ListBucketResult>`, basePrefix)
				return
			}
			key := basePrefix + "folder/parts/0"
			_, _ = fmt.Fprintf(w, `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>next</NextContinuationToken><Contents><Key>%s</Key><LastModified>2026-07-29T00:00:00Z</LastModified><Size>3</Size></Contents></ListBucketResult>`, key)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	adapter, err := newTestS3Adapter(t, server.URL)
	require.NoError(t, err)
	ctx := context.Background()

	size, err := adapter.InspectIndexedFolder(ctx, "folder/parts", 2)
	require.NoError(t, err)
	require.Equal(t, int64(7), size)
	sizes, err := adapter.InspectIndexedFolderSizes(ctx, "folder/parts", 2)
	require.NoError(t, err)
	require.Equal(t, []int64{3, 4}, sizes)
	var folders []string
	require.NoError(t, adapter.WalkTopLevelFolders(ctx, func(folderName string) error {
		folders = append(folders, folderName)
		return nil
	}))
	require.Equal(t, []string{"folder"}, folders)

	count, err := adapter.CountFilesInFolder(ctx, "folder/parts")
	require.NoError(t, err)
	require.Equal(t, 2, count)
	require.Equal(t, 3, continuationRequests)
}

func TestS3WalkTopLevelFoldersStreamsUnorderedPrefixesAcrossPages(t *testing.T) {
	const basePrefix = "gh-actions-cache/"
	continuationRequests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Query().Get("list-type") != "2" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		if r.URL.Query().Get("max-keys") == "1" {
			_, _ = fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
			return
		}
		if r.URL.Query().Get("prefix") != basePrefix || r.URL.Query().Get("delimiter") != "/" {
			http.Error(w, "unexpected listing scope", http.StatusBadRequest)
			return
		}
		if r.URL.Query().Get("continuation-token") == "next" {
			continuationRequests++
			_, _ = fmt.Fprintf(w, `<ListBucketResult><IsTruncated>false</IsTruncated><CommonPrefixes><Prefix>%sb/</Prefix></CommonPrefixes></ListBucketResult>`, basePrefix)
			return
		}
		_, _ = fmt.Fprintf(w, `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>next</NextContinuationToken><CommonPrefixes><Prefix>%sz/</Prefix></CommonPrefixes><CommonPrefixes><Prefix>%sa/</Prefix></CommonPrefixes></ListBucketResult>`, basePrefix, basePrefix)
	}))
	defer server.Close()
	adapter, err := newTestS3Adapter(t, server.URL)
	require.NoError(t, err)

	var folders []string
	require.NoError(t, adapter.WalkTopLevelFolders(context.Background(), func(folderName string) error {
		folders = append(folders, folderName)
		return nil
	}))
	require.Equal(t, []string{"z", "a", "b"}, folders)
	require.Equal(t, 1, continuationRequests)
}

func TestS3DeleteFolderDeletesBoundedFirstPagesUntilEmpty(t *testing.T) {
	const objectCount = s3DeleteBatchSize + 1
	remaining := make([]string, 0, objectCount)
	for index := 0; index < objectCount; index++ {
		remaining = append(remaining, fmt.Sprintf("gh-actions-cache/folder/object-%04d", index))
	}
	listCalls := 0
	deleteCalls := 0
	maxDeleteBatch := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
			w.Header().Set("Content-Type", "application/xml")
			if r.URL.Query().Get("max-keys") == "1" {
				_, _ = fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
				return
			}
			listCalls++
			pageSize := min(len(remaining), s3DeleteBatchSize)
			_, _ = fmt.Fprintf(w, `<ListBucketResult><IsTruncated>%t</IsTruncated>`, len(remaining) > pageSize)
			for _, key := range remaining[:pageSize] {
				_, _ = fmt.Fprintf(w, `<Contents><Key>%s</Key></Contents>`, key)
			}
			_, _ = fmt.Fprint(w, `</ListBucketResult>`)
			return
		}
		if r.Method == http.MethodPost && hasQueryKey(r, "delete") {
			deleteCalls++
			var request struct {
				Objects []struct {
					Key string `xml:"Key"`
				} `xml:"Object"`
			}
			if err := xml.NewDecoder(r.Body).Decode(&request); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
			maxDeleteBatch = max(maxDeleteBatch, len(request.Objects))
			deleted := make(map[string]struct{}, len(request.Objects))
			for _, object := range request.Objects {
				deleted[object.Key] = struct{}{}
			}
			kept := remaining[:0]
			for _, key := range remaining {
				if _, ok := deleted[key]; !ok {
					kept = append(kept, key)
				}
			}
			remaining = kept
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprint(w, `<DeleteResult/>`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	adapter, err := newTestS3Adapter(t, server.URL)
	require.NoError(t, err)

	require.NoError(t, adapter.DeleteFolder(context.Background(), "folder"))
	require.Empty(t, remaining)
	require.Equal(t, 3, listCalls)
	require.Equal(t, 2, deleteCalls)
	require.Equal(t, s3DeleteBatchSize, maxDeleteBatch)
}

func TestS3DeleteFolderStopsWhenSuccessfulDeletesMakeNoProgress(t *testing.T) {
	const key = "gh-actions-cache/folder/object"
	listCalls := 0
	deleteCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
			w.Header().Set("Content-Type", "application/xml")
			if r.URL.Query().Get("max-keys") == "1" {
				_, _ = fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
				return
			}
			listCalls++
			_, _ = fmt.Fprintf(w, `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>%s</Key></Contents></ListBucketResult>`, key)
			return
		}
		if r.Method == http.MethodPost && hasQueryKey(r, "delete") {
			deleteCalls++
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprint(w, `<DeleteResult/>`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	adapter, err := newTestS3Adapter(t, server.URL)
	require.NoError(t, err)

	err = adapter.DeleteFolder(context.Background(), "folder")
	require.ErrorContains(t, err, "listing made no progress")
	require.Equal(t, 4, listCalls)
	require.Equal(t, 3, deleteCalls)
}

func TestS3WalkTopLevelFoldersRejectsMultiTokenCycle(t *testing.T) {
	consumerListCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Query().Get("list-type") != "2" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		if r.URL.Query().Get("max-keys") == "1" {
			_, _ = fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
			return
		}
		consumerListCalls++
		nextToken := cyclicTestToken(r.URL.Query().Get("continuation-token"))
		_, _ = fmt.Fprintf(w, `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>%s</NextContinuationToken><CommonPrefixes><Prefix>gh-actions-cache/folder/</Prefix></CommonPrefixes></ListBucketResult>`, nextToken)
	}))
	defer server.Close()
	adapter, err := newTestS3Adapter(t, server.URL)
	require.NoError(t, err)

	err = adapter.WalkTopLevelFolders(context.Background(), func(string) error { return nil })
	require.ErrorContains(t, err, "cyclic continuation token")
	require.Equal(t, 4, consumerListCalls)
}

func TestS3SharedListingRejectsMultiTokenCycle(t *testing.T) {
	consumerListCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Query().Get("list-type") != "2" {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		if r.URL.Query().Get("max-keys") == "1" {
			_, _ = fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
			return
		}
		consumerListCalls++
		nextToken := cyclicTestToken(r.URL.Query().Get("continuation-token"))
		key := r.URL.Query().Get("prefix") + strconv.Itoa(consumerListCalls)
		_, _ = fmt.Fprintf(w, `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>%s</NextContinuationToken><Contents><Key>%s</Key><LastModified>2026-07-29T00:00:00Z</LastModified><Size>1</Size></Contents></ListBucketResult>`, nextToken, key)
	}))
	defer server.Close()
	adapter, err := newTestS3Adapter(t, server.URL)
	require.NoError(t, err)

	_, err = adapter.InspectFolderSummary(context.Background(), "folder")
	require.ErrorContains(t, err, "cyclic continuation token")
	require.Equal(t, 4, consumerListCalls)
}

func TestS3DeleteFolderRejectsEmptyTruncatedPage(t *testing.T) {
	deleteCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
			w.Header().Set("Content-Type", "application/xml")
			if r.URL.Query().Get("max-keys") == "1" {
				_, _ = fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
				return
			}
			_, _ = fmt.Fprint(w, `<ListBucketResult><IsTruncated>true</IsTruncated><NextContinuationToken>opaque</NextContinuationToken></ListBucketResult>`)
			return
		}
		if r.Method == http.MethodPost && hasQueryKey(r, "delete") {
			deleteCalls++
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	adapter, err := newTestS3Adapter(t, server.URL)
	require.NoError(t, err)

	err = adapter.DeleteFolder(context.Background(), "folder")
	require.ErrorContains(t, err, "truncated response contains no objects")
	require.Zero(t, deleteCalls)
}

func TestS3DeleteFolderRejectsAlternatingPageCycle(t *testing.T) {
	listCalls := 0
	deleteCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
			w.Header().Set("Content-Type", "application/xml")
			if r.URL.Query().Get("max-keys") == "1" {
				_, _ = fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
				return
			}
			listCalls++
			key := "gh-actions-cache/folder/a"
			if listCalls%2 == 0 {
				key = "gh-actions-cache/folder/b"
			}
			_, _ = fmt.Fprintf(w, `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>%s</Key></Contents></ListBucketResult>`, key)
			return
		}
		if r.Method == http.MethodPost && hasQueryKey(r, "delete") {
			deleteCalls++
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprint(w, `<DeleteResult/>`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	adapter, err := newTestS3Adapter(t, server.URL)
	require.NoError(t, err)

	err = adapter.DeleteFolder(context.Background(), "folder")
	require.ErrorContains(t, err, "listing entered a page cycle")
	require.Equal(t, 4, listCalls)
	require.Equal(t, 3, deleteCalls)
}

func TestS3DeleteByPrefixStopsAtHardPageLimit(t *testing.T) {
	listCalls := 0
	deleteCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodGet && r.URL.Query().Get("list-type") == "2" {
			w.Header().Set("Content-Type", "application/xml")
			if r.URL.Query().Get("max-keys") == "1" {
				_, _ = fmt.Fprint(w, `<ListBucketResult><IsTruncated>false</IsTruncated></ListBucketResult>`)
				return
			}
			listCalls++
			_, _ = fmt.Fprintf(w, `<ListBucketResult><IsTruncated>false</IsTruncated><Contents><Key>gh-actions-cache/folder/object-%d</Key></Contents></ListBucketResult>`, listCalls)
			return
		}
		if r.Method == http.MethodPost && hasQueryKey(r, "delete") {
			deleteCalls++
			w.Header().Set("Content-Type", "application/xml")
			_, _ = fmt.Fprint(w, `<DeleteResult/>`)
			return
		}
		w.WriteHeader(http.StatusNotFound)
	}))
	defer server.Close()
	adapter, err := newTestS3Adapter(t, server.URL)
	require.NoError(t, err)

	err = adapter.deleteByPrefixWithPageLimit(context.Background(), "gh-actions-cache/folder/", 2)
	require.ErrorContains(t, err, "reached work limit after 2 pages")
	require.Equal(t, 2, listCalls)
	require.Equal(t, 2, deleteCalls)
}

func cyclicTestToken(current string) string {
	switch current {
	case "A":
		return "B"
	case "B":
		return "A"
	default:
		return "A"
	}
}

func validPaginationTestPrefix(prefix, delimiter, basePrefix string) bool {
	return prefix == basePrefix && delimiter == "" ||
		prefix == basePrefix+"folder/parts/" && delimiter == "" ||
		prefix == basePrefix+"folder/parts/" && delimiter == "/" ||
		prefix == basePrefix+"folder/" && delimiter == ""
}

func TestS3AdapterCopyObjectUsesServerSideCopy(t *testing.T) {
	ctx := context.Background()
	fakeS3 := newFakeS3Server(t, fakeS3Options{})
	defer fakeS3.Close()
	adapter, err := newTestS3Adapter(t, fakeS3.URL)
	require.NoError(t, err)

	require.NoError(t, adapter.CopyObject(ctx, "folder/blocks/source", "folder/parts/0"))
	require.Equal(t, "cache-bucket%2Fgh-actions-cache%2Ffolder%2Fblocks%2Fsource", fakeS3.copiedSource())
	require.Equal(t, "/cache-bucket/gh-actions-cache/folder/parts/0", fakeS3.copiedDestination())
	require.False(t, fakeS3.putObjectCalled())
}

func TestS3AdapterComposeObjectsUsesMultipartServerSideCopies(t *testing.T) {
	ctx := context.Background()
	fakeS3 := newFakeS3Server(t, fakeS3Options{objectSizes: map[string]int64{
		"/cache-bucket/gh-actions-cache/folder/parts/0": s3MinimumComposePartSize,
		"/cache-bucket/gh-actions-cache/folder/parts/1": 1,
	}})
	defer fakeS3.Close()
	adapter, err := newTestS3Adapter(t, fakeS3.URL)
	require.NoError(t, err)

	requestsBefore := fakeS3.requestCountValue()
	require.NoError(t, adapter.ComposeObjects(ctx, "folder/merged", []string{"folder/parts/0", "folder/parts/1"}))
	require.Equal(t, s3TwoSourceComposeRequestCount, fakeS3.requestCountValue()-requestsBefore)
	require.Equal(t, []string{
		"/cache-bucket/gh-actions-cache/folder/parts/0",
		"/cache-bucket/gh-actions-cache/folder/parts/1",
	}, fakeS3.headObjectPaths())
	require.Equal(t, []int{1, 2}, fakeS3.copiedPartNumbers())
	require.Equal(t, []string{
		"cache-bucket%2Fgh-actions-cache%2Ffolder%2Fparts%2F0",
		"cache-bucket%2Fgh-actions-cache%2Ffolder%2Fparts%2F1",
	}, fakeS3.copiedPartSources())
	require.Equal(t, []string{"", ""}, fakeS3.copiedPartRanges())
	require.True(t, fakeS3.completeMultipartCalled())
	require.False(t, fakeS3.abortMultipartCalled())
}

func TestS3AdapterComposeObjectsRejectsSmallNonFinalSource(t *testing.T) {
	ctx := context.Background()
	fakeS3 := newFakeS3Server(t, fakeS3Options{objectSizes: map[string]int64{
		"/cache-bucket/gh-actions-cache/folder/parts/0": 1,
		"/cache-bucket/gh-actions-cache/folder/parts/1": s3MinimumComposePartSize,
	}})
	defer fakeS3.Close()
	adapter, err := newTestS3Adapter(t, fakeS3.URL)
	require.NoError(t, err)

	err = adapter.ComposeObjects(ctx, "folder/merged", []string{"folder/parts/0", "folder/parts/1"})
	require.ErrorIs(t, err, ErrComposeUnsupported)
	require.False(t, fakeS3.createMultipartCalled())
}

func TestS3AdapterComposeObjectsAbortsFailedMultipartCopy(t *testing.T) {
	ctx := context.Background()
	fakeS3 := newFakeS3Server(t, fakeS3Options{
		failPartNumber: 2,
		objectSizes: map[string]int64{
			"/cache-bucket/gh-actions-cache/folder/parts/0": s3MinimumComposePartSize,
			"/cache-bucket/gh-actions-cache/folder/parts/1": 1,
		},
	})
	defer fakeS3.Close()
	adapter, err := newTestS3Adapter(t, fakeS3.URL)
	require.NoError(t, err)

	err = adapter.ComposeObjects(ctx, "folder/merged", []string{"folder/parts/0", "folder/parts/1"})
	require.Error(t, err)
	require.True(t, fakeS3.abortMultipartCalled())
	require.False(t, fakeS3.completeMultipartCalled())
}

func TestS3AdapterComposeObjectsAbortsCanceledMultipartCopyWithFreshContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	fakeS3 := newFakeS3Server(t, fakeS3Options{
		onCopyPart: func(partNumber int) {
			if partNumber == 1 {
				cancel()
			}
		},
		objectSizes: map[string]int64{
			"/cache-bucket/gh-actions-cache/folder/parts/0": s3MinimumComposePartSize,
			"/cache-bucket/gh-actions-cache/folder/parts/1": 1,
		},
	})
	defer fakeS3.Close()
	adapter, err := newTestS3Adapter(t, fakeS3.URL)
	require.NoError(t, err)

	err = adapter.ComposeObjects(ctx, "folder/merged", []string{"folder/parts/0", "folder/parts/1"})
	require.Error(t, err)
	require.True(t, fakeS3.abortMultipartCalled())
	require.False(t, fakeS3.completeMultipartCalled())
}

func TestPlanS3ComposePartsSplitsLargeSourceIntoBalancedRanges(t *testing.T) {
	sourceSize := s3MaximumComposePartSize + s3MinimumComposePartSize
	parts, err := planS3ComposeParts([]string{"large"}, []int64{sourceSize})
	require.NoError(t, err)
	require.Len(t, parts, 2)
	require.Equal(t, int64(0), parts[0].start)
	require.Equal(t, parts[0].size, parts[1].start)
	require.Equal(t, sourceSize, parts[0].size+parts[1].size)
	require.GreaterOrEqual(t, parts[0].size, s3MinimumComposePartSize)
	require.GreaterOrEqual(t, parts[1].size, s3MinimumComposePartSize)
	require.LessOrEqual(t, parts[0].size, s3MaximumComposePartSize)
	require.LessOrEqual(t, parts[1].size, s3MaximumComposePartSize)
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
	headBucketStatus    int
	listObjectsStatus   int
	headObjectStatus    int
	headObjectErrorCode string
	getObjectStatus     int
	getObjectErrorCode  string
	failPartNumber      int
	onUploadPart        func(int)
	onCopyPart          func(int)
	objectSizes         map[string]int64
}

type fakeS3Server struct {
	*httptest.Server
	headBucketStatus    int
	listObjectsStatus   int
	headObjectStatus    int
	headObjectErrorCode string
	getObjectStatus     int
	getObjectErrorCode  string
	failPartNumber      int
	onUploadPart        func(int)
	onCopyPart          func(int)
	objectSizes         map[string]int64

	mu              sync.Mutex
	requestCount    int
	headBucket      int
	listObjects     int
	listPrefix      string
	listMaxKeys     string
	headObjects     []string
	putObject       bool
	copySource      string
	copyDestination string
	createUpload    bool
	uploadParts     []int
	uploadPartSize  []int
	copyParts       []int
	copyPartSources []string
	copyPartRanges  []string
	completeUpload  bool
	abortUpload     bool
}

func newFakeS3Server(t testing.TB, options fakeS3Options) *fakeS3Server {
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
		headBucketStatus:    options.headBucketStatus,
		listObjectsStatus:   options.listObjectsStatus,
		headObjectStatus:    options.headObjectStatus,
		headObjectErrorCode: options.headObjectErrorCode,
		getObjectStatus:     options.getObjectStatus,
		getObjectErrorCode:  options.getObjectErrorCode,
		failPartNumber:      options.failPartNumber,
		onUploadPart:        options.onUploadPart,
		onCopyPart:          options.onCopyPart,
		objectSizes:         options.objectSizes,
	}
	fakeS3.Server = httptest.NewServer(http.HandlerFunc(fakeS3.handle))
	return fakeS3
}

func (s *fakeS3Server) handle(w http.ResponseWriter, r *http.Request) {
	s.mu.Lock()
	s.requestCount++
	s.mu.Unlock()

	switch {
	case r.Method == http.MethodHead && r.URL.Path == "/cache-bucket":
		s.mu.Lock()
		s.headBucket++
		s.mu.Unlock()
		w.WriteHeader(s.headBucketStatus)
	case r.Method == http.MethodHead && strings.HasPrefix(r.URL.Path, "/cache-bucket/"):
		s.mu.Lock()
		s.headObjects = append(s.headObjects, r.URL.Path)
		size, ok := s.objectSizes[r.URL.Path]
		s.mu.Unlock()
		if s.headObjectStatus != 0 && s.headObjectStatus != http.StatusOK {
			code := s.headObjectErrorCode
			if code == "" {
				code = "InternalError"
			}
			w.WriteHeader(s.headObjectStatus)
			_, _ = fmt.Fprintf(w, `<Error><Code>%s</Code><Message>head failed</Message></Error>`, code)
			return
		}
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			_, _ = fmt.Fprint(w, `<Error><Code>NoSuchKey</Code><Message>object not found</Message></Error>`)
			return
		}
		w.Header().Set("Content-Length", strconv.FormatInt(size, 10))
		w.WriteHeader(http.StatusOK)
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
	case r.Method == http.MethodPut && r.URL.Query().Get("uploadId") == "upload-id" && r.Header.Get("X-Amz-Copy-Source") != "":
		partNumber, _ := strconv.Atoi(r.URL.Query().Get("partNumber"))
		s.mu.Lock()
		s.copyParts = append(s.copyParts, partNumber)
		s.copyPartSources = append(s.copyPartSources, r.Header.Get("X-Amz-Copy-Source"))
		s.copyPartRanges = append(s.copyPartRanges, r.Header.Get("X-Amz-Copy-Source-Range"))
		failPartNumber := s.failPartNumber
		onCopyPart := s.onCopyPart
		s.mu.Unlock()
		if onCopyPart != nil {
			onCopyPart(partNumber)
		}
		if partNumber == failPartNumber {
			w.WriteHeader(http.StatusInternalServerError)
			_, _ = fmt.Fprint(w, `<Error><Code>InternalError</Code><Message>copy part failed</Message></Error>`)
			return
		}
		w.Header().Set("Content-Type", "application/xml")
		_, _ = fmt.Fprintf(w, `<CopyPartResult><ETag>"copy-part-%d"</ETag><LastModified>2026-07-27T00:00:00.000Z</LastModified></CopyPartResult>`, partNumber)
	case r.Method == http.MethodPut && r.Header.Get("X-Amz-Copy-Source") != "":
		s.mu.Lock()
		s.copySource = r.Header.Get("X-Amz-Copy-Source")
		s.copyDestination = r.URL.Path
		s.mu.Unlock()
		w.Header().Set("Content-Type", "application/xml")
		_, _ = fmt.Fprint(w, `<CopyObjectResult><ETag>"copy"</ETag><LastModified>2026-07-27T00:00:00.000Z</LastModified></CopyObjectResult>`)
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

func (s *fakeS3Server) requestCountValue() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.requestCount
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

func (s *fakeS3Server) headObjectPaths() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.headObjects...)
}

func (s *fakeS3Server) copiedPartNumbers() []int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]int(nil), s.copyParts...)
}

func (s *fakeS3Server) copiedPartSources() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.copyPartSources...)
}

func (s *fakeS3Server) copiedPartRanges() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]string(nil), s.copyPartRanges...)
}

func (s *fakeS3Server) copiedSource() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.copySource
}

func (s *fakeS3Server) copiedDestination() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.copyDestination
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

func newTestS3Adapter(t testing.TB, endpoint string) (*S3Adapter, error) {
	t.Helper()

	return newTestS3AdapterWithConfig(t, config.StorageConfig{S3EndpointURL: endpoint})
}

func newTestS3AdapterWithConfig(t testing.TB, cfg config.StorageConfig) (*S3Adapter, error) {
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
