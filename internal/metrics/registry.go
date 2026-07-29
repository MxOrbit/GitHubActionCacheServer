package metrics

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/storagedeletion"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/storagelocation"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const databaseCollectionTimeout = 5 * time.Second

type Recorder interface {
	RecordCacheRequest(hit bool)
	RecordCacheUpload()
	RecordStorageDeletionFailure()
}

type Registry struct {
	registry                *prometheus.Registry
	cacheRequests           *prometheus.CounterVec
	cacheUploads            prometheus.Counter
	storageDeletionFailures prometheus.Counter
}

func New(db *ent.Client) *Registry {
	return newRegistry(db, time.Now)
}

func newRegistry(db *ent.Client, now func() time.Time) *Registry {
	prometheusRegistry := prometheus.NewRegistry()
	cacheRequests := prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "cache_requests_total",
		Help: "Cache download-URL lookups by final result. Restore-key prefix matches count as hits.",
	}, []string{"result"})
	cacheUploads := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "cache_uploads_total",
		Help: "Cache uploads finalized into cache entries.",
	})
	storageDeletionFailures := prometheus.NewCounter(prometheus.CounterOpts{
		Name: "storage_deletion_failures_total",
		Help: "Retryable physical storage deletion failures observed by this process.",
	})

	// Materialize both bounded result series so dashboards see zeros before the
	// first lookup on a newly started replica.
	cacheRequests.WithLabelValues("hit").Add(0)
	cacheRequests.WithLabelValues("miss").Add(0)

	prometheusRegistry.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
		cacheRequests,
		cacheUploads,
		storageDeletionFailures,
		newDatabaseCollector(db, now),
	)

	return &Registry{
		registry:                prometheusRegistry,
		cacheRequests:           cacheRequests,
		cacheUploads:            cacheUploads,
		storageDeletionFailures: storageDeletionFailures,
	}
}

func (r *Registry) Handler() http.Handler {
	return promhttp.HandlerFor(r.registry, promhttp.HandlerOpts{
		ErrorHandling: promhttp.HTTPErrorOnError,
	})
}

func (r *Registry) RecordCacheRequest(hit bool) {
	result := "miss"
	if hit {
		result = "hit"
	}
	r.cacheRequests.WithLabelValues(result).Inc()
}

func (r *Registry) RecordCacheUpload() {
	r.cacheUploads.Inc()
}

func (r *Registry) RecordStorageDeletionFailure() {
	r.storageDeletionFailures.Inc()
}

type nopRecorder struct{}

func NopRecorder() Recorder {
	return nopRecorder{}
}

func (nopRecorder) RecordCacheRequest(bool) {}

func (nopRecorder) RecordCacheUpload() {}

func (nopRecorder) RecordStorageDeletionFailure() {}

type databaseCollector struct {
	db                           *ent.Client
	now                          func() time.Time
	cacheStorageBytes            *prometheus.Desc
	storageDeletionsPending      *prometheus.Desc
	storageDeletionOldestAgeSecs *prometheus.Desc
}

func newDatabaseCollector(db *ent.Client, now func() time.Time) *databaseCollector {
	return &databaseCollector{
		db:  db,
		now: now,
		cacheStorageBytes: prometheus.NewDesc(
			"cache_storage_bytes",
			"Logical payload bytes tracked by storage-location metadata, including fenced rows until lifecycle finalization.",
			nil,
			nil,
		),
		storageDeletionsPending: prometheus.NewDesc(
			"storage_deletions_pending",
			"Physical storage deletion tasks currently pending in the durable outbox.",
			nil,
			nil,
		),
		storageDeletionOldestAgeSecs: prometheus.NewDesc(
			"storage_deletion_oldest_age_seconds",
			"Age in seconds of the oldest pending physical storage deletion task, or zero when none are pending.",
			nil,
			nil,
		),
	}
}

func (c *databaseCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.cacheStorageBytes
	ch <- c.storageDeletionsPending
	ch <- c.storageDeletionOldestAgeSecs
}

func (c *databaseCollector) Collect(ch chan<- prometheus.Metric) {
	ctx, cancel := context.WithTimeout(context.Background(), databaseCollectionTimeout)
	defer cancel()

	storageBytes, err := c.collectCacheStorageBytes(ctx)
	if err != nil {
		ch <- prometheus.NewInvalidMetric(c.cacheStorageBytes, err)
	} else {
		ch <- prometheus.MustNewConstMetric(c.cacheStorageBytes, prometheus.GaugeValue, float64(storageBytes))
	}

	pending, oldestCreatedAt, err := c.collectStorageDeletionSnapshot(ctx)
	if err != nil {
		wrapped := fmt.Errorf("collect storage deletion outbox metrics: %w", err)
		ch <- prometheus.NewInvalidMetric(c.storageDeletionsPending, wrapped)
		ch <- prometheus.NewInvalidMetric(c.storageDeletionOldestAgeSecs, wrapped)
		return
	}

	oldestAgeSeconds := 0.0
	if oldestCreatedAt != nil {
		age := max(0, c.now().UnixMilli()-*oldestCreatedAt)
		oldestAgeSeconds = float64(age) / float64(time.Second/time.Millisecond)
	}
	ch <- prometheus.MustNewConstMetric(c.storageDeletionsPending, prometheus.GaugeValue, float64(pending))
	ch <- prometheus.MustNewConstMetric(c.storageDeletionOldestAgeSecs, prometheus.GaugeValue, oldestAgeSeconds)
}

func (c *databaseCollector) collectCacheStorageBytes(ctx context.Context) (int64, error) {
	var totals []struct {
		Bytes *int64 `json:"bytes"`
	}
	if err := c.db.StorageLocation.Query().
		Aggregate(ent.As(ent.Sum(storagelocation.FieldSizeBytes), "bytes")).
		Scan(ctx, &totals); err != nil {
		return 0, fmt.Errorf("sum tracked cache payload sizes: %w", err)
	}
	if len(totals) == 0 || totals[0].Bytes == nil {
		return 0, nil
	}
	return *totals[0].Bytes, nil
}

func (c *databaseCollector) collectStorageDeletionSnapshot(ctx context.Context) (int64, *int64, error) {
	var snapshots []struct {
		Pending         int64  `json:"pending"`
		OldestCreatedAt *int64 `json:"oldestCreatedAt"`
	}
	if err := c.db.StorageDeletion.Query().
		Aggregate(
			ent.As(ent.Count(), "pending"),
			ent.As(ent.Min(storagedeletion.FieldCreatedAt), "oldestCreatedAt"),
		).
		Scan(ctx, &snapshots); err != nil {
		return 0, nil, err
	}
	if len(snapshots) == 0 {
		return 0, nil, nil
	}
	return snapshots[0].Pending, snapshots[0].OldestCreatedAt, nil
}
