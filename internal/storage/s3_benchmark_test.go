package storage

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
)

func BenchmarkS3ComposeObjects(b *testing.B) {
	const finalPartSize int64 = 1024
	sources := []string{"folder/parts/0", "folder/parts/1"}
	logicalBytes := s3MinimumComposePartSize + finalPartSize

	for _, directDownloads := range []bool{false, true} {
		b.Run(fmt.Sprintf("direct-downloads=%t", directDownloads), func(b *testing.B) {
			fakeS3 := newFakeS3Server(b, fakeS3Options{objectSizes: map[string]int64{
				"/cache-bucket/gh-actions-cache/folder/parts/0": s3MinimumComposePartSize,
				"/cache-bucket/gh-actions-cache/folder/parts/1": finalPartSize,
			}})
			defer fakeS3.Close()
			adapter, err := newTestS3Adapter(b, fakeS3.URL)
			if err != nil {
				b.Fatal(err)
			}
			requestsBefore := fakeS3.requestCountValue()

			b.ReportAllocs()
			b.SetBytes(logicalBytes)
			iteration := 0
			for b.Loop() {
				destination := fmt.Sprintf("folder/merged-%d", iteration)
				iteration++
				if err := adapter.ComposeObjects(context.Background(), destination, sources); err != nil {
					b.Fatal(err)
				}
				if directDownloads {
					if _, err := adapter.CreateDownloadURL(context.Background(), destination, 10*time.Minute); err != nil {
						b.Fatal(err)
					}
				}
			}

			requestCount := fakeS3.requestCountValue() - requestsBefore
			expectedRequests := s3TwoSourceComposeRequestCount * b.N
			if requestCount != expectedRequests {
				b.Fatalf("compose issued %d requests, expected %d", requestCount, expectedRequests)
			}
			b.ReportMetric(float64(requestCount)/float64(b.N), "requests/op")
		})
	}
}

func BenchmarkS3ComposeObjectsExternal(b *testing.B) {
	endpoint := strings.TrimSpace(os.Getenv("E2E_S3_ENDPOINT_URL"))
	bucket := strings.TrimSpace(os.Getenv("E2E_S3_BUCKET"))
	if endpoint == "" || bucket == "" {
		b.Skip("set E2E_S3_ENDPOINT_URL and E2E_S3_BUCKET to benchmark external S3 composition")
	}
	region := strings.TrimSpace(os.Getenv("E2E_S3_REGION"))
	if region == "" {
		region = "us-east-1"
	}

	ctx := context.Background()
	adapter, err := NewS3Adapter(ctx, config.StorageConfig{
		S3Bucket:         bucket,
		S3Region:         region,
		S3EndpointURL:    endpoint,
		S3ForcePathStyle: true,
		S3KeyPrefix:      fmt.Sprintf("gh-actions-cache-benchmark-%d", time.Now().UnixNano()),
	})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := adapter.Clear(cleanupCtx); err != nil {
			b.Error(err)
		}
	})

	sources := []string{"sources/0", "sources/1"}
	if err := adapter.UploadStream(ctx, sources[0], bytes.NewReader(make([]byte, s3MinimumComposePartSize))); err != nil {
		b.Fatal(err)
	}
	const finalPartSize = 1024
	if err := adapter.UploadStream(ctx, sources[1], bytes.NewReader(make([]byte, finalPartSize))); err != nil {
		b.Fatal(err)
	}
	logicalBytes := s3MinimumComposePartSize + finalPartSize

	for _, directDownloads := range []bool{false, true} {
		b.Run(fmt.Sprintf("direct-downloads=%t", directDownloads), func(b *testing.B) {
			b.ReportAllocs()
			b.SetBytes(logicalBytes)
			iteration := 0
			for b.Loop() {
				destination := fmt.Sprintf("merged/%d-%d", time.Now().UnixNano(), iteration)
				iteration++
				if err := adapter.ComposeObjects(ctx, destination, sources); err != nil {
					b.Fatal(err)
				}
				if directDownloads {
					if _, err := adapter.CreateDownloadURL(ctx, destination, 10*time.Minute); err != nil {
						b.Fatal(err)
					}
				}
			}
		})
	}
}
