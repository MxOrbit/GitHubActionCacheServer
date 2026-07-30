package e2e

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
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

func TestManagementCacheEntryFilters(t *testing.T) {
	t.Setenv("MANAGEMENT_API_KEY", "secret")

	for _, rpc := range []bool{false, true} {
		name := "http"
		if rpc {
			name = "rpc"
		}
		t.Run(name, func(t *testing.T) {
			app := newTestApp(t)
			seedManagementFilterEntries(app)

			filters := map[string]any{
				"key":     "target-key",
				"version": "target-version",
				"scope":   "target-scope",
				"repoId":  "target-repo",
			}
			listRec := managementCacheEntryListRequest(t, app.router, rpc, filters)
			require.Equal(t, http.StatusOK, listRec.Code)
			listResponse := decodeManagementCacheEntryList(t, listRec, rpc)
			require.Equal(t, 1, listResponse.Total)
			require.Equal(t, []string{"target"}, managementCacheEntryResponseIDs(listResponse))

			deleteRec := managementCacheEntryDeleteManyRequest(t, app.router, rpc, filters)
			if rpc {
				require.Equal(t, http.StatusOK, deleteRec.Code)
				require.JSONEq(t, `{"json":null,"meta":[[3]]}`, deleteRec.Body.String())
			} else {
				require.Equal(t, http.StatusNoContent, deleteRec.Code)
			}

			require.ElementsMatch(t,
				[]string{"wrong-key", "wrong-version", "wrong-scope", "wrong-repo"},
				app.db.CacheEntry.Query().IDsX(context.Background()),
			)
		})
	}
}

func TestManagementMatchCoercesSingleAndArrayInputs(t *testing.T) {
	t.Setenv("MANAGEMENT_API_KEY", "secret")

	for _, rpc := range []bool{false, true} {
		transport := "http"
		if rpc {
			transport = "rpc"
		}
		t.Run(transport, func(t *testing.T) {
			app := newTestApp(t)
			seedManagementMatchEntries(app)

			tests := []struct {
				name        string
				primaryKey  string
				restoreKeys any
				scopes      any
				version     string
				matchType   string
				matchID     string
			}{
				{name: "primary", primaryKey: "primary-hit", scopes: "scope-2", version: "target-version", matchType: "exact-primary", matchID: "primary-target"},
				{name: "single", primaryKey: "primary-miss", restoreKeys: "restore-hit", scopes: "scope-2", version: "target-version", matchType: "prefixed-restore", matchID: "target"},
				{name: "array", primaryKey: "primary-miss", restoreKeys: []string{"restore-miss", "restore-hit"}, scopes: []string{"scope-1", "scope-2"}, version: "target-version", matchType: "prefixed-restore", matchID: "target"},
				{name: "scope-order", primaryKey: "primary-miss", restoreKeys: "scope-order-key", scopes: []string{"scope-1", "scope-2"}, version: "scope-order-version", matchType: "exact-restore", matchID: "first-scope"},
				{name: "restore-key-order", primaryKey: "primary-miss", restoreKeys: []string{"restore-first", "restore-second"}, scopes: "restore-order-scope", version: "restore-order-version", matchType: "exact-restore", matchID: "first-restore-key"},
			}
			for _, tt := range tests {
				t.Run(tt.name, func(t *testing.T) {
					input := map[string]any{
						"primaryKey": tt.primaryKey,
						"scopes":     tt.scopes,
						"version":    tt.version,
						"repoId":     "target-repo",
					}
					if tt.restoreKeys != nil {
						input["restoreKeys"] = tt.restoreKeys
					}
					rec := managementCacheEntryMatchRequest(t, app.router, rpc, input)
					require.Equal(t, http.StatusOK, rec.Code)
					match := decodeManagementCacheEntryMatch(t, rec, rpc)
					require.Equal(t, tt.matchType, match.Type)
					require.Equal(t, tt.matchID, match.Match.ID)
				})
			}
		})
	}
}

func TestManagementListPaginationCoercion(t *testing.T) {
	t.Setenv("MANAGEMENT_API_KEY", "secret")

	for _, rpc := range []bool{false, true} {
		transport := "http"
		if rpc {
			transport = "rpc"
		}
		t.Run(transport, func(t *testing.T) {
			app := newTestApp(t)
			createManagementCacheEntry(app, "oldest", "key-oldest", "version", "scope", "repo", 100)
			createManagementCacheEntry(app, "middle", "key-middle", "version", "scope", "repo", 200)
			createManagementCacheEntry(app, "newest", "key-newest", "version", "scope", "repo", 300)

			inputs := []map[string]any{{"page": "2", "itemsPerPage": "1"}}
			if rpc {
				inputs = append(inputs, map[string]any{"page": 2, "itemsPerPage": 1})
			}
			for _, input := range inputs {
				rec := managementCacheEntryListRequest(t, app.router, rpc, input)
				require.Equal(t, http.StatusOK, rec.Code)
				response := decodeManagementCacheEntryList(t, rec, rpc)
				require.Equal(t, 3, response.Total)
				require.Equal(t, []string{"middle"}, managementCacheEntryResponseIDs(response))
			}
		})
	}
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

type managementCacheEntryListResponse struct {
	Total int `json:"total"`
	Items []struct {
		ID string `json:"id"`
	} `json:"items"`
}

type managementCacheEntryMatchResponse struct {
	Type  string `json:"type"`
	Match struct {
		ID string `json:"id"`
	} `json:"match"`
}

func seedManagementFilterEntries(app testApp) {
	createManagementCacheEntry(app, "target", "target-key", "target-version", "target-scope", "target-repo", 100)
	createManagementCacheEntry(app, "wrong-key", "wrong-key", "target-version", "target-scope", "target-repo", 200)
	createManagementCacheEntry(app, "wrong-version", "target-key", "wrong-version", "target-scope", "target-repo", 300)
	createManagementCacheEntry(app, "wrong-scope", "target-key", "target-version", "wrong-scope", "target-repo", 400)
	createManagementCacheEntry(app, "wrong-repo", "target-key", "target-version", "target-scope", "wrong-repo", 500)
}

func seedManagementMatchEntries(app testApp) {
	createManagementCacheEntry(app, "wrong-version", "restore-hit", "wrong-version", "scope-2", "target-repo", 100)
	createManagementCacheEntry(app, "wrong-repo", "restore-hit", "target-version", "scope-2", "wrong-repo", 200)
	createManagementCacheEntry(app, "wrong-scope", "restore-hit", "target-version", "wrong-scope", "target-repo", 300)
	createManagementCacheEntry(app, "target", "restore-hit-target", "target-version", "scope-2", "target-repo", 400)
	createManagementCacheEntry(app, "primary-target", "primary-hit", "target-version", "scope-2", "target-repo", 500)
	createManagementCacheEntry(app, "first-scope", "scope-order-key", "scope-order-version", "scope-1", "target-repo", 600)
	createManagementCacheEntry(app, "second-scope", "scope-order-key", "scope-order-version", "scope-2", "target-repo", 700)
	createManagementCacheEntry(app, "first-restore-key", "restore-first", "restore-order-version", "restore-order-scope", "target-repo", 800)
	createManagementCacheEntry(app, "second-restore-key", "restore-second", "restore-order-version", "restore-order-scope", "target-repo", 900)
}

func createManagementCacheEntry(app testApp, id string, key string, version string, scope string, repoID string, updatedAt int64) {
	location := app.db.StorageLocation.Create().
		SetID(id + "-location").
		SetFolderName(id + "-folder").
		SetPartCount(1).
		SaveX(context.Background())
	app.db.CacheEntry.Create().
		SetID(id).
		SetKey(key).
		SetVersion(version).
		SetScope(scope).
		SetRepoId(repoID).
		SetUpdatedAt(updatedAt).
		SetLocation(location).
		SaveX(context.Background())
}

func managementCacheEntryListRequest(t *testing.T, router http.Handler, rpc bool, input map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	if rpc {
		return managementRPCRequest(t, router, "cacheEntries/findMany", input)
	}
	return managementRequest(router, http.MethodGet, "/management-api/cache-entries/?"+managementQuery(t, input).Encode(), "secret")
}

func managementCacheEntryDeleteManyRequest(t *testing.T, router http.Handler, rpc bool, input map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	if rpc {
		return managementRPCRequest(t, router, "cacheEntries/deleteMany", input)
	}
	return managementRequest(router, http.MethodDelete, "/management-api/cache-entries/?"+managementQuery(t, input).Encode(), "secret")
}

func managementCacheEntryMatchRequest(t *testing.T, router http.Handler, rpc bool, input map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	if rpc {
		return managementRPCRequest(t, router, "cacheEntries/match", input)
	}
	return managementRequest(router, http.MethodGet, "/management-api/cache-entries/match?"+managementQuery(t, input).Encode(), "secret")
}

func managementRPCRequest(t *testing.T, router http.Handler, procedure string, input map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	body, err := json.Marshal(map[string]any{"json": input, "meta": []any{}})
	require.NoError(t, err)
	req := httptest.NewRequest(http.MethodPost, "/management-api/_rpc/"+procedure, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-key", "secret")
	rec := httptest.NewRecorder()
	router.ServeHTTP(rec, req)
	return rec
}

func managementQuery(t *testing.T, input map[string]any) url.Values {
	t.Helper()
	query := url.Values{}
	for key, value := range input {
		switch value := value.(type) {
		case string:
			query.Add(key, value)
		case []string:
			for _, item := range value {
				query.Add(key, item)
			}
		default:
			t.Fatalf("unsupported management query value %T", value)
		}
	}
	return query
}

func decodeManagementCacheEntryList(t *testing.T, rec *httptest.ResponseRecorder, rpc bool) managementCacheEntryListResponse {
	t.Helper()
	var response managementCacheEntryListResponse
	decodeManagementResponse(t, rec, rpc, &response)
	return response
}

func decodeManagementCacheEntryMatch(t *testing.T, rec *httptest.ResponseRecorder, rpc bool) managementCacheEntryMatchResponse {
	t.Helper()
	var response managementCacheEntryMatchResponse
	decodeManagementResponse(t, rec, rpc, &response)
	return response
}

func decodeManagementResponse(t *testing.T, rec *httptest.ResponseRecorder, rpc bool, target any) {
	t.Helper()
	if !rpc {
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), target))
		return
	}
	var envelope struct {
		JSON json.RawMessage `json:"json"`
		Meta []any           `json:"meta"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &envelope))
	require.Empty(t, envelope.Meta)
	require.NoError(t, json.Unmarshal(envelope.JSON, target))
}

func managementCacheEntryResponseIDs(response managementCacheEntryListResponse) []string {
	ids := make([]string, 0, len(response.Items))
	for _, item := range response.Items {
		ids = append(ids, item.ID)
	}
	return ids
}
