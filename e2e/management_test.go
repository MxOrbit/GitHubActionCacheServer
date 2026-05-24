package e2e

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

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

	deleteRec := managementRequest(app.router, http.MethodDelete, "/management-api/cache-entries/entry-id", "secret")
	require.Equal(t, http.StatusNoContent, deleteRec.Code)

	getDeletedRec := managementRequest(app.router, http.MethodGet, "/management-api/cache-entries/entry-id", "secret")
	require.Equal(t, http.StatusNotFound, getDeletedRec.Code)
}

func TestManagementStorageLocations(t *testing.T) {
	t.Setenv("MANAGEMENT_API_KEY", "secret")
	app := newTestApp(t)
	app.db.StorageLocation.Create().
		SetID("location-id").
		SetFolderName("folder").
		SetPartCount(1).
		SaveX(context.Background())

	getRec := managementRequest(app.router, http.MethodGet, "/management-api/storage-locations/location-id", "secret")
	require.Equal(t, http.StatusOK, getRec.Code)

	deleteRec := managementRequest(app.router, http.MethodDelete, "/management-api/storage-locations/location-id", "secret")
	require.Equal(t, http.StatusNoContent, deleteRec.Code)

	getDeletedRec := managementRequest(app.router, http.MethodGet, "/management-api/storage-locations/location-id", "secret")
	require.Equal(t, http.StatusNotFound, getDeletedRec.Code)
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
