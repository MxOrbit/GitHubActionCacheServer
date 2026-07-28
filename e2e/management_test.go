package e2e

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/storagelifecycle"
	"github.com/stretchr/testify/require"
)

func TestManagementAPIRequiresEnablementAndAPIKey(t *testing.T) {
	disabledApp := newTestApp(t)
	req := httptest.NewRequest(http.MethodGet, "/management-api/cache-entries/", nil)
	rec := httptest.NewRecorder()
	disabledApp.router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusServiceUnavailable, rec.Code)

	t.Setenv("MANAGEMENT_API_KEY", "secret")
	app := newTestApp(t)
	req = httptest.NewRequest(http.MethodGet, "/management-api/cache-entries/", nil)
	rec = httptest.NewRecorder()
	app.router.ServeHTTP(rec, req)
	require.Equal(t, http.StatusUnauthorized, rec.Code)
}

func TestManagementCacheEntries(t *testing.T) {
	t.Setenv("MANAGEMENT_API_KEY", "secret")
	app := newTestApp(t)
	location := app.db.StorageLocation.Create().
		SetID("location-id").
		SetFolderName("folder").
		SetPartCount(1).
		SaveX(context.Background())
	app.db.CacheEntry.Create().
		SetID("entry-id").
		SetKey("linux-cache-old").
		SetVersion("version-1").
		SetScope("refs/heads/main").
		SetRepoId("123").
		SetUpdatedAt(time.Now().UnixMilli()).
		SetLocation(location).
		SaveX(context.Background())

	listRec := managementRequest(app.router, http.MethodGet, "/management-api/cache-entries/?key=linux-cache-old", "secret")
	require.Equal(t, http.StatusOK, listRec.Code)
	var listResponse struct {
		Total int `json:"total"`
		Items []struct {
			ID  string `json:"id"`
			Key string `json:"key"`
		} `json:"items"`
	}
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResponse))
	require.Equal(t, 1, listResponse.Total)
	require.Equal(t, "entry-id", listResponse.Items[0].ID)

	getRec := managementRequest(app.router, http.MethodGet, "/management-api/cache-entries/entry-id", "secret")
	require.Equal(t, http.StatusOK, getRec.Code)

	matchRec := managementRequest(app.router, http.MethodGet, "/management-api/cache-entries/match?primaryKey=missing&restoreKeys=linux-cache&scopes=refs/heads/main&repoId=123&version=version-1", "secret")
	require.Equal(t, http.StatusOK, matchRec.Code)
	var matchResponse struct {
		Type  string `json:"type"`
		Match struct {
			ID  string `json:"id"`
			Key string `json:"key"`
		} `json:"match"`
	}
	require.NoError(t, json.Unmarshal(matchRec.Body.Bytes(), &matchResponse))
	require.Equal(t, "prefixed-restore", matchResponse.Type)
	require.Equal(t, "entry-id", matchResponse.Match.ID)

	prefixedPrimaryLocation := app.db.StorageLocation.Create().
		SetID("prefixed-primary-location").
		SetFolderName("prefixed-primary-folder").
		SetPartCount(1).
		SaveX(context.Background())
	app.db.CacheEntry.Create().
		SetID("prefixed-primary-entry").
		SetKey("primary-cache-new").
		SetVersion("version-1").
		SetScope("refs/heads/main").
		SetRepoId("123").
		SetUpdatedAt(time.Now().Add(time.Second).UnixMilli()).
		SetLocation(prefixedPrimaryLocation).
		SaveX(context.Background())
	exactRestoreLocation := app.db.StorageLocation.Create().
		SetID("exact-restore-location").
		SetFolderName("exact-restore-folder").
		SetPartCount(1).
		SaveX(context.Background())
	app.db.CacheEntry.Create().
		SetID("exact-restore-entry").
		SetKey("restore-cache").
		SetVersion("version-1").
		SetScope("refs/heads/main").
		SetRepoId("123").
		SetUpdatedAt(time.Now().UnixMilli()).
		SetLocation(exactRestoreLocation).
		SaveX(context.Background())

	matchRec = managementRequest(app.router, http.MethodGet, "/management-api/cache-entries/match?primaryKey=primary-cache&restoreKeys=restore-cache&scopes=refs/heads/main&repoId=123&version=version-1", "secret")
	require.Equal(t, http.StatusOK, matchRec.Code)
	require.NoError(t, json.Unmarshal(matchRec.Body.Bytes(), &matchResponse))
	require.Equal(t, "prefixed-primary", matchResponse.Type)
	require.Equal(t, "prefixed-primary-entry", matchResponse.Match.ID)

	deleteRec := managementRequest(app.router, http.MethodDelete, "/management-api/cache-entries/entry-id", "secret")
	require.Equal(t, http.StatusNoContent, deleteRec.Code)

	getDeletedRec := managementRequest(app.router, http.MethodGet, "/management-api/cache-entries/entry-id", "secret")
	require.Equal(t, http.StatusNotFound, getDeletedRec.Code)
}

func TestManagementStorageLocations(t *testing.T) {
	t.Setenv("MANAGEMENT_API_KEY", "secret")
	app := newTestApp(t)
	location := app.db.StorageLocation.Create().
		SetID("location-id").
		SetFolderName("folder").
		SetPartCount(1).
		SaveX(context.Background())
	app.db.CacheEntry.Create().
		SetID("entry-id").
		SetKey("key").
		SetVersion("version").
		SetScope("scope").
		SetRepoId("repo").
		SetUpdatedAt(time.Now().UnixMilli()).
		SetLocation(location).
		SaveX(context.Background())
	lease, err := app.lifecycle.AcquireReader(context.Background(), "entry-id", storagelifecycle.AcquireReaderOptions{})
	require.NoError(t, err)

	getRec := managementRequest(app.router, http.MethodGet, "/management-api/storage-locations/location-id", "secret")
	require.Equal(t, http.StatusOK, getRec.Code)

	deleteRec := managementRequest(app.router, http.MethodDelete, "/management-api/storage-locations/location-id", "secret")
	require.Equal(t, http.StatusNoContent, deleteRec.Code)

	getDeletedRec := managementRequest(app.router, http.MethodGet, "/management-api/storage-locations/location-id", "secret")
	require.Equal(t, http.StatusNotFound, getDeletedRec.Code)
	require.NotNil(t, app.db.StorageLocation.GetX(context.Background(), "location-id").DeletionRequestedAt)
	require.Zero(t, app.db.CacheEntry.Query().CountX(context.Background()))
	require.Equal(t, 1, app.db.StorageReaderLease.Query().CountX(context.Background()))
	require.NoError(t, app.lifecycle.ReleaseReader(context.Background(), lease.ID))
}

func managementRequest(router http.Handler, method string, path string, apiKey string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, nil)
	if apiKey != "" {
		req.Header.Set("x-api-key", apiKey)
	}
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}
