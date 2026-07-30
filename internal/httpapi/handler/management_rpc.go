package handler

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/cachekey"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/cacheentry"
	entpredicate "github.com/MxOrbit/GitHubActionCacheServer/internal/ent/predicate"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/storagelocation"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/managementauth"
	"github.com/gin-gonic/gin"
)

type managementRPCEnvelope struct {
	JSON any   `json:"json"`
	Meta []any `json:"meta"`
}

type managementRPCError struct {
	Defined bool           `json:"defined"`
	Code    string         `json:"code"`
	Status  int            `json:"status"`
	Message string         `json:"message"`
	Data    map[string]any `json:"data"`
}

func (h *Handler) ManagementRPC(c *gin.Context) {
	if h.cfg.Management.APIKey == "" {
		managementRPCErrorResponse(c, http.StatusServiceUnavailable, "SERVICE_UNAVAILABLE", "management api is disabled", true)
		return
	}
	if !managementauth.Matches(h.cfg.Management.APIKey, c.GetHeader("x-api-key")) {
		managementRPCErrorResponse(c, http.StatusUnauthorized, "UNAUTHORIZED", "unauthorized", true)
		return
	}

	procedure := managementRPCProcedure(c)
	if procedure == "" {
		managementRPCErrorResponse(c, http.StatusNotFound, "NOT_FOUND", "management rpc procedure not found", true)
		return
	}

	input, err := decodeManagementRPCInput(c)
	if err != nil {
		managementRPCErrorResponse(c, http.StatusBadRequest, "BAD_REQUEST", err.Error(), true)
		return
	}

	switch procedure {
	case "cacheEntries.findMany":
		h.managementRPCListCacheEntries(c, input)
	case "cacheEntries.get":
		h.managementRPCGetCacheEntry(c, input)
	case "cacheEntries.match":
		h.managementRPCMatchCacheEntry(c, input)
	case "cacheEntries.delete":
		h.managementRPCDeleteCacheEntry(c, input)
	case "cacheEntries.deleteMany":
		h.managementRPCDeleteCacheEntries(c, input)
	case "storageLocations.get":
		h.managementRPCGetStorageLocation(c, input)
	case "storageLocations.delete":
		h.managementRPCDeleteStorageLocation(c, input)
	default:
		managementRPCErrorResponse(c, http.StatusNotFound, "NOT_FOUND", "management rpc procedure not found", true)
	}
}

func (h *Handler) managementRPCListCacheEntries(c *gin.Context, input map[string]any) {
	filters := managementRPCCacheEntryFilters(input)
	page := rpcPositiveInt(input, "page", 1, 0)
	itemsPerPage := rpcPositiveInt(input, "itemsPerPage", defaultManagementItemsPerPage, maxManagementItemsPerPage)

	result, err := h.listManagementCacheEntries(c, filters, page, itemsPerPage)
	if err != nil {
		managementRPCErrorResponse(c, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR", err.Error(), false)
		return
	}

	managementRPCSuccess(c, http.StatusOK, result)
}

func (h *Handler) managementRPCGetCacheEntry(c *gin.Context, input map[string]any) {
	id, ok := rpcRequiredString(input, "id")
	if !ok {
		managementRPCErrorResponse(c, http.StatusBadRequest, "BAD_REQUEST", "missing required input: id", true)
		return
	}

	entry, err := h.db.CacheEntry.Query().
		Where(cacheentry.ID(id)).
		Only(c.Request.Context())
	if err != nil {
		if ent.IsNotFound(err) {
			managementRPCErrorResponse(c, http.StatusNotFound, "NOT_FOUND", "cache entry not found", true)
			return
		}
		managementRPCErrorResponse(c, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR", err.Error(), false)
		return
	}

	managementRPCSuccess(c, http.StatusOK, entry)
}

func (h *Handler) managementRPCMatchCacheEntry(c *gin.Context, input map[string]any) {
	primaryKey, primaryOK := rpcRequiredString(input, "primaryKey")
	version, versionOK := rpcRequiredString(input, "version")
	repoID, repoOK := rpcRequiredString(input, "repoId")
	scopes := rpcStringSlice(input, "scopes")
	restoreKeys := nonBlankStrings(rpcStringSlice(input, "restoreKeys"))
	if !primaryOK || !versionOK || !repoOK || len(scopes) == 0 {
		managementRPCErrorResponse(c, http.StatusBadRequest, "BAD_REQUEST", "missing required input", true)
		return
	}

	match, matchType, err := h.matchManagementCacheEntry(c, primaryKey, restoreKeys, version, scopes, repoID)
	if err != nil {
		managementRPCErrorResponse(c, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR", err.Error(), false)
		return
	}
	if match == nil {
		managementRPCSuccess(c, http.StatusOK, nil)
		return
	}

	managementRPCSuccess(c, http.StatusOK, cacheEntryMatchResponse{Match: match, Type: matchType})
}

func (h *Handler) managementRPCDeleteCacheEntry(c *gin.Context, input map[string]any) {
	id, ok := rpcRequiredString(input, "id")
	if !ok {
		managementRPCErrorResponse(c, http.StatusBadRequest, "BAD_REQUEST", "missing required input: id", true)
		return
	}

	if err := h.deleteManagementCacheEntry(c, id); err != nil {
		managementRPCErrorResponse(c, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR", err.Error(), false)
		return
	}

	managementRPCUndefined(c)
}

func (h *Handler) managementRPCDeleteCacheEntries(c *gin.Context, input map[string]any) {
	if err := h.deleteManagementCacheEntries(c.Request.Context(), managementRPCCacheEntryFilters(input)); err != nil {
		managementRPCErrorResponse(c, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR", err.Error(), false)
		return
	}

	managementRPCUndefined(c)
}

func (h *Handler) managementRPCGetStorageLocation(c *gin.Context, input map[string]any) {
	id, ok := rpcRequiredString(input, "id")
	if !ok {
		managementRPCErrorResponse(c, http.StatusBadRequest, "BAD_REQUEST", "missing required input: id", true)
		return
	}

	location, err := h.db.StorageLocation.Query().
		Where(
			storagelocation.ID(id),
			storagelocation.DeletionRequestedAtIsNil(),
		).
		Only(c.Request.Context())
	if err != nil {
		if ent.IsNotFound(err) {
			managementRPCErrorResponse(c, http.StatusNotFound, "NOT_FOUND", "storage location not found", true)
			return
		}
		managementRPCErrorResponse(c, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR", err.Error(), false)
		return
	}

	managementRPCSuccess(c, http.StatusOK, location)
}

func (h *Handler) managementRPCDeleteStorageLocation(c *gin.Context, input map[string]any) {
	id, ok := rpcRequiredString(input, "id")
	if !ok {
		managementRPCErrorResponse(c, http.StatusBadRequest, "BAD_REQUEST", "missing required input: id", true)
		return
	}

	if err := h.deleteManagementStorageLocation(c, id); err != nil {
		managementRPCErrorResponse(c, http.StatusInternalServerError, "INTERNAL_SERVER_ERROR", err.Error(), false)
		return
	}

	managementRPCUndefined(c)
}

func managementRPCProcedure(c *gin.Context) string {
	procedure := c.Param("procedure")
	if procedure == "" {
		procedure = strings.TrimPrefix(c.Request.URL.Path, "/management-api/_rpc")
	}

	procedure = strings.Trim(procedure, "/")
	procedure = strings.ReplaceAll(procedure, "/", ".")
	return procedure
}

func decodeManagementRPCInput(c *gin.Context) (map[string]any, error) {
	if data := strings.TrimSpace(c.Query("data")); data != "" {
		return decodeManagementRPCData(strings.NewReader(data))
	}
	if c.Request.Body == nil {
		return map[string]any{}, nil
	}

	return decodeManagementRPCData(c.Request.Body)
}

func decodeManagementRPCData(data io.Reader) (map[string]any, error) {
	decoder := json.NewDecoder(data)
	decoder.UseNumber()

	var payload any
	if err := decoder.Decode(&payload); err != nil {
		if errors.Is(err, io.EOF) {
			return map[string]any{}, nil
		}
		return nil, err
	}

	if object, ok := payload.(map[string]any); ok {
		if value, ok := object["json"]; ok {
			payload = value
		} else if value, ok := object["input"]; ok {
			payload = value
		} else if value, ok := object["params"]; ok {
			payload = value
		}
	}

	if payload == nil {
		return map[string]any{}, nil
	}
	input, ok := payload.(map[string]any)
	if !ok {
		return nil, errors.New("rpc input must be an object")
	}
	return input, nil
}

func managementRPCCacheEntryFilters(input map[string]any) []entpredicate.CacheEntry {
	var filters []entpredicate.CacheEntry
	if value := rpcString(input, "key"); value != "" {
		filters = append(filters, cachekey.Exact(value))
	}
	if value := rpcString(input, "version"); value != "" {
		filters = append(filters, cacheentry.Version(value))
	}
	if value := rpcString(input, "scope"); value != "" {
		filters = append(filters, cacheentry.Scope(value))
	}
	if value := rpcString(input, "repoId"); value != "" {
		filters = append(filters, cacheentry.RepoId(value))
	}
	return filters
}

func rpcRequiredString(input map[string]any, key string) (string, bool) {
	value := strings.TrimSpace(rpcString(input, key))
	return value, value != ""
}

func rpcString(input map[string]any, key string) string {
	value, ok := input[key]
	if !ok || value == nil {
		return ""
	}

	switch typed := value.(type) {
	case string:
		return typed
	case json.Number:
		return typed.String()
	case float64:
		return strconv.FormatFloat(typed, 'f', -1, 64)
	case bool:
		return strconv.FormatBool(typed)
	default:
		return fmt.Sprint(typed)
	}
}

func rpcStringSlice(input map[string]any, key string) []string {
	value, ok := input[key]
	if !ok || value == nil {
		return nil
	}

	switch typed := value.(type) {
	case []string:
		return typed
	case string:
		if typed == "" {
			return nil
		}
		return []string{typed}
	case []any:
		values := make([]string, 0, len(typed))
		for _, item := range typed {
			switch item := item.(type) {
			case string:
				values = append(values, item)
			case json.Number:
				values = append(values, item.String())
			default:
				if item != nil {
					values = append(values, fmt.Sprint(item))
				}
			}
		}
		return values
	default:
		return []string{fmt.Sprint(typed)}
	}
}

func rpcPositiveInt(input map[string]any, key string, fallback int, max int) int {
	value, ok := input[key]
	if !ok || value == nil {
		return fallback
	}

	var parsed int
	switch typed := value.(type) {
	case json.Number:
		intValue, err := strconv.Atoi(typed.String())
		if err != nil {
			return fallback
		}
		parsed = intValue
	case float64:
		parsed = int(typed)
	case int:
		parsed = typed
	case string:
		intValue, err := strconv.Atoi(typed)
		if err != nil {
			return fallback
		}
		parsed = intValue
	default:
		return fallback
	}

	if parsed < 1 {
		return fallback
	}
	if max > 0 && parsed > max {
		return max
	}
	return parsed
}

func managementRPCSuccess(c *gin.Context, status int, payload any) {
	c.JSON(status, managementRPCEnvelope{
		JSON: payload,
		Meta: []any{},
	})
}

func managementRPCUndefined(c *gin.Context) {
	c.JSON(http.StatusOK, managementRPCEnvelope{
		JSON: nil,
		Meta: []any{[]any{3}},
	})
}

func managementRPCErrorResponse(c *gin.Context, status int, code string, message string, defined bool) {
	c.JSON(status, managementRPCEnvelope{
		JSON: managementRPCError{
			Defined: defined,
			Code:    code,
			Status:  status,
			Message: message,
			Data:    map[string]any{},
		},
		Meta: []any{},
	})
}
