package metrics

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestRegistryExposesCountersAndDatabaseGauges(t *testing.T) {
	ctx, client := testutil.NewSQLiteClient(t)
	now := time.UnixMilli(2_000_000)

	client.StorageLocation.Create().
		SetID("active-location").
		SetFolderName("active-folder").
		SetPartCount(1).
		SetSizeBytes(11).
		SaveX(ctx)
	client.StorageLocation.Create().
		SetID("fenced-location").
		SetFolderName("fenced-folder").
		SetPartCount(1).
		SetSizeBytes(13).
		SetDeletionRequestedAt(now.Add(-time.Minute).UnixMilli()).
		SaveX(ctx)
	client.StorageLocation.Create().
		SetID("unreconciled-location").
		SetFolderName("unreconciled-folder").
		SetPartCount(1).
		SaveX(ctx)
	client.StorageDeletion.Create().
		SetFolderName("oldest-folder").
		SetCreatedAt(now.Add(-10 * time.Second).UnixMilli()).
		SaveX(ctx)
	client.StorageDeletion.Create().
		SetFolderName("newer-folder").
		SetCreatedAt(now.Add(-5 * time.Second).UnixMilli()).
		SaveX(ctx)

	registry := newRegistry(client, func() time.Time { return now })
	require.Equal(t, float64(0), gatheredMetricValue(t, registry, "cache_requests_total", map[string]string{"result": "hit"}))
	require.Equal(t, float64(0), gatheredMetricValue(t, registry, "cache_requests_total", map[string]string{"result": "miss"}))
	require.Equal(t, float64(24), gatheredMetricValue(t, registry, "cache_storage_bytes", nil))
	require.Equal(t, float64(2), gatheredMetricValue(t, registry, "storage_deletions_pending", nil))
	require.Equal(t, float64(10), gatheredMetricValue(t, registry, "storage_deletion_oldest_age_seconds", nil))

	registry.RecordCacheRequest(true)
	registry.RecordCacheRequest(false)
	registry.RecordCacheUpload()
	registry.RecordStorageDeletionFailure()

	require.Equal(t, float64(1), gatheredMetricValue(t, registry, "cache_requests_total", map[string]string{"result": "hit"}))
	require.Equal(t, float64(1), gatheredMetricValue(t, registry, "cache_requests_total", map[string]string{"result": "miss"}))
	require.Equal(t, float64(1), gatheredMetricValue(t, registry, "cache_uploads_total", nil))
	require.Equal(t, float64(1), gatheredMetricValue(t, registry, "storage_deletion_failures_total", nil))
}

func TestHandlerExposesPrometheusTextWithRuntimeMetrics(t *testing.T) {
	_, client := testutil.NewSQLiteClient(t)
	registry := New(client)
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	rec := httptest.NewRecorder()

	registry.Handler().ServeHTTP(rec, req)

	require.Equal(t, http.StatusOK, rec.Code)
	require.Contains(t, rec.Header().Get("Content-Type"), "text/plain")
	require.Contains(t, rec.Body.String(), "go_goroutines")
	require.Contains(t, rec.Body.String(), "process_cpu_seconds_total")
}

func gatheredMetricValue(t *testing.T, registry *Registry, name string, expectedLabels map[string]string) float64 {
	t.Helper()
	families, err := registry.registry.Gather()
	require.NoError(t, err)
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			labels := make(map[string]string, len(metric.GetLabel()))
			for _, label := range metric.GetLabel() {
				labels[label.GetName()] = label.GetValue()
			}
			if !equalLabels(labels, expectedLabels) {
				continue
			}
			if metric.Counter != nil {
				return metric.GetCounter().GetValue()
			}
			if metric.Gauge != nil {
				return metric.GetGauge().GetValue()
			}
			t.Fatalf("metric %q is neither a counter nor a gauge", name)
		}
	}
	t.Fatalf("metric %q with labels %v not found", name, expectedLabels)
	return 0
}

func equalLabels(actual, expected map[string]string) bool {
	if len(actual) != len(expected) {
		return false
	}
	for name, value := range expected {
		if actual[name] != value {
			return false
		}
	}
	return true
}
