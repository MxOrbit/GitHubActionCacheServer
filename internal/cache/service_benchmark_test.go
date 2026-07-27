package cache

import (
	"bytes"
	"context"
	"fmt"
	"runtime"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/auth"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/bufferpool"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/testutil"
)

const (
	benchmarkPayloadSize = 4 * 1024 * 1024
	benchmarkPartCount   = 4
	benchmarkSampleLimit = 65_536
)

var benchmarkBufferSizes = []int{
	32 * 1024,
	128 * 1024,
	256 * 1024,
	1024 * 1024,
}

func BenchmarkFilesystemWholeCache(b *testing.B) {
	b.Run("upload", func(b *testing.B) {
		forEachBenchmarkBufferSize(b, benchmarkFilesystemWholeCacheUpload)
	})
	b.Run("download", func(b *testing.B) {
		forEachBenchmarkBufferSize(b, benchmarkFilesystemWholeCacheDownload)
	})
}

// BenchmarkAzureBlockCommit exercises the Azure-style block protocol on the filesystem backend, without Azure Blob Storage.
func BenchmarkAzureBlockCommit(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B) {
		ctx, _, _, service := newFilesystemBenchmarkService(b)
		payload := bytes.Repeat([]byte{0xa5}, benchmarkPayloadSize/benchmarkPartCount)
		blockIDs := make([]string, benchmarkPartCount)
		for index := range blockIDs {
			blockIDs[index] = fmt.Sprintf("block-%d", index)
		}
		scope := benchmarkScope()

		b.ReportAllocs()
		b.SetBytes(benchmarkPayloadSize)
		iteration := 0
		for b.Loop() {
			key := fmt.Sprintf("azure-block-%d", iteration)
			iteration++
			upload, err := service.CreateUpload(ctx, key, "benchmark-version", scope)
			if err != nil {
				b.Fatal(err)
			}
			for _, blockID := range blockIDs {
				if err := service.UploadBlock(ctx, upload.UploadID, blockID, bytes.NewReader(payload)); err != nil {
					b.Fatal(err)
				}
			}
			if err := service.CommitBlockList(ctx, upload.UploadID, blockIDs); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkFilesystemDownloadRepresentation(b *testing.B) {
	for _, representation := range []string{"parts", "merged"} {
		b.Run(representation, func(b *testing.B) {
			forEachBenchmarkBufferSize(b, func(b *testing.B) {
				ctx, client, filesystem, service := newFilesystemBenchmarkService(b)
				entryID := prepareBenchmarkDownload(
					b,
					ctx,
					client,
					filesystem,
					representation,
					bytes.Repeat([]byte{0x5a}, benchmarkPayloadSize),
				)

				b.ReportAllocs()
				b.SetBytes(benchmarkPayloadSize)
				for b.Loop() {
					if err := consumeBenchmarkDownload(ctx, service, entryID, benchmarkPayloadSize); err != nil {
						b.Fatal(err)
					}
				}
			})
		})
	}
}

func BenchmarkFilesystemConcurrentRunners(b *testing.B) {
	forEachBenchmarkBufferSize(b, func(b *testing.B) {
		ctx, client, filesystem, service := newFilesystemBenchmarkService(b)
		entryID := prepareBenchmarkDownload(
			b,
			ctx,
			client,
			filesystem,
			"parts",
			bytes.Repeat([]byte{0x3c}, benchmarkPayloadSize),
		)

		sampleLimit := b.N
		if sampleLimit > benchmarkSampleLimit {
			sampleLimit = benchmarkSampleLimit
		}
		latencies := make([]int64, sampleLimit)
		var sampleCount atomic.Uint64
		var firstErr error
		var errOnce sync.Once

		initialRSS := processRSSBytes()
		var peakRSS atomic.Uint64
		peakRSS.Store(initialRSS)
		rssDone := make(chan struct{})
		var rssWG sync.WaitGroup
		rssWG.Add(1)
		go func() {
			defer rssWG.Done()
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()
			for {
				select {
				case <-rssDone:
					return
				case <-ticker.C:
					updatePeakRSS(&peakRSS, processRSSBytes())
				}
			}
		}()

		b.ReportAllocs()
		b.SetBytes(benchmarkPayloadSize)
		b.SetParallelism(2)
		b.ResetTimer()
		b.RunParallel(func(parallel *testing.PB) {
			for parallel.Next() {
				started := time.Now()
				err := consumeBenchmarkDownload(ctx, service, entryID, benchmarkPayloadSize)
				elapsed := time.Since(started).Nanoseconds()
				if err != nil {
					errOnce.Do(func() { firstErr = err })
				}
				index := sampleCount.Add(1) - 1
				if index < uint64(len(latencies)) {
					latencies[index] = elapsed
				}
			}
		})
		b.StopTimer()

		close(rssDone)
		rssWG.Wait()
		updatePeakRSS(&peakRSS, processRSSBytes())
		if firstErr != nil {
			b.Fatal(firstErr)
		}
		if len(latencies) > 0 {
			sort.Slice(latencies, func(i, j int) bool { return latencies[i] < latencies[j] })
			p95Index := (len(latencies)*95+99)/100 - 1
			b.ReportMetric(float64(latencies[p95Index]), "p95-ns")
		}
		if rss := peakRSS.Load(); rss > 0 {
			b.ReportMetric(float64(rss), "peak-RSS-bytes")
		}
		b.ReportMetric(float64(2*runtime.GOMAXPROCS(0)), "runners")
	})
}

func benchmarkFilesystemWholeCacheUpload(b *testing.B) {
	ctx, _, _, service := newFilesystemBenchmarkService(b)
	payload := bytes.Repeat([]byte{0xc3}, benchmarkPayloadSize)
	scope := benchmarkScope()

	b.ReportAllocs()
	b.SetBytes(benchmarkPayloadSize)
	iteration := 0
	for b.Loop() {
		key := fmt.Sprintf("whole-cache-%d", iteration)
		iteration++
		upload, err := service.CreateUpload(ctx, key, "benchmark-version", scope)
		if err != nil {
			b.Fatal(err)
		}
		if err := service.UploadPart(ctx, upload.UploadID, bytes.NewReader(payload)); err != nil {
			b.Fatal(err)
		}
		if _, err := service.CompleteUpload(ctx, key, "benchmark-version", scope); err != nil {
			b.Fatal(err)
		}
	}
}

func benchmarkFilesystemWholeCacheDownload(b *testing.B) {
	ctx, client, _, service := newFilesystemBenchmarkService(b)
	entryID := prepareBenchmarkWholeCache(
		b,
		ctx,
		client,
		service,
		bytes.Repeat([]byte{0x96}, benchmarkPayloadSize),
	)

	b.ReportAllocs()
	b.SetBytes(benchmarkPayloadSize)
	for b.Loop() {
		if err := consumeBenchmarkDownload(ctx, service, entryID, benchmarkPayloadSize); err != nil {
			b.Fatal(err)
		}
	}
}

func prepareBenchmarkWholeCache(
	b *testing.B,
	ctx context.Context,
	client *ent.Client,
	service *Service,
	payload []byte,
) string {
	b.Helper()

	const key = "whole-cache-download"
	const version = "benchmark-version"
	scope := benchmarkScope()
	upload, err := service.CreateUpload(ctx, key, version, scope)
	if err != nil {
		b.Fatal(err)
	}
	if err := service.UploadPart(ctx, upload.UploadID, bytes.NewReader(payload)); err != nil {
		b.Fatal(err)
	}
	if _, err := service.CompleteUpload(ctx, key, version, scope); err != nil {
		b.Fatal(err)
	}
	entry, err := service.MatchCacheEntry(ctx, []string{key}, version, scope)
	if err != nil {
		b.Fatal(err)
	}
	if entry == nil {
		b.Fatal("completed cache entry was not found")
	}
	if err := client.StorageLocation.UpdateOneID(entry.LocationId).
		SetLastDownloadedAt(time.Now().UnixMilli()).
		Exec(ctx); err != nil {
		b.Fatal(err)
	}
	return entry.ID
}

func forEachBenchmarkBufferSize(b *testing.B, run func(*testing.B)) {
	for _, size := range benchmarkBufferSizes {
		b.Run(fmt.Sprintf("buffer=%dKiB", size/1024), func(b *testing.B) {
			previous := bufferpool.Default
			bufferpool.Default = bufferpool.New(size)
			defer func() { bufferpool.Default = previous }()
			run(b)
		})
	}
}

func newFilesystemBenchmarkService(b *testing.B) (context.Context, *ent.Client, *storage.FilesystemAdapter, *Service) {
	b.Helper()

	ctx, client, filesystem := testutil.NewSQLiteFilesystem(b)
	service := NewService(Options{DB: client, Storage: filesystem, MergeConcurrency: 1})
	b.Cleanup(func() {
		service.StopAcceptingMerges()
		waitCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		if err := service.WaitForMerges(waitCtx); err != nil {
			b.Error(err)
		}
	})
	return ctx, client, filesystem, service
}

func prepareBenchmarkDownload(
	b *testing.B,
	ctx context.Context,
	client *ent.Client,
	filesystem *storage.FilesystemAdapter,
	representation string,
	payload []byte,
) string {
	b.Helper()

	entryID := "benchmark-" + representation
	folderName := entryID + "-folder"
	partSize := len(payload) / benchmarkPartCount
	partCount := benchmarkPartCount
	if representation == "merged" {
		if err := filesystem.UploadStream(ctx, mergedObjectName(folderName), bytes.NewReader(payload)); err != nil {
			b.Fatal(err)
		}
	} else {
		for index := 0; index < benchmarkPartCount; index++ {
			start := index * partSize
			end := start + partSize
			if index == benchmarkPartCount-1 {
				end = len(payload)
			}
			if err := filesystem.UploadStream(ctx, partObjectName(folderName, index), bytes.NewReader(payload[start:end])); err != nil {
				b.Fatal(err)
			}
		}
	}

	locationCreate := client.StorageLocation.Create().
		SetID(entryID + "-location").
		SetFolderName(folderName).
		SetPartCount(partCount).
		SetLastDownloadedAt(time.Now().UnixMilli())
	if representation == "merged" {
		locationCreate.SetMergedAt(time.Now().UnixMilli())
	}
	location, err := locationCreate.Save(ctx)
	if err != nil {
		b.Fatal(err)
	}
	_, err = client.CacheEntry.Create().
		SetID(entryID).
		SetKey(entryID + "-key").
		SetVersion("benchmark-version").
		SetScope("refs/heads/main").
		SetRepoId("benchmark-repo").
		SetUpdatedAt(time.Now().UnixMilli()).
		SetLocation(location).
		Save(ctx)
	if err != nil {
		b.Fatal(err)
	}
	return entryID
}

func consumeBenchmarkDownload(ctx context.Context, service *Service, entryID string, expectedBytes int) error {
	stream, err := service.Download(ctx, entryID)
	if err != nil {
		return err
	}
	written, copyErr := bufferpool.Copy(benchmarkDiscardWriter{}, stream)
	closeErr := stream.Close()
	if copyErr != nil {
		return copyErr
	}
	if closeErr != nil {
		return closeErr
	}
	if written != int64(expectedBytes) {
		return fmt.Errorf("downloaded %d bytes, expected %d", written, expectedBytes)
	}
	return nil
}

func benchmarkScope() auth.CacheScope {
	return auth.CacheScope{
		RepoID: "benchmark-repo",
		Scopes: []auth.Scope{{Scope: "refs/heads/main", Permission: 3}},
	}
}

func updatePeakRSS(peak *atomic.Uint64, rss uint64) {
	for current := peak.Load(); rss > current; current = peak.Load() {
		if peak.CompareAndSwap(current, rss) {
			return
		}
	}
}

type benchmarkDiscardWriter struct{}

func (benchmarkDiscardWriter) Write(p []byte) (int, error) {
	return len(p), nil
}
