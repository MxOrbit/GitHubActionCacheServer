package handler

import (
	"context"
	stdsql "database/sql"
	"errors"
	"fmt"
	"net/http"
	"strconv"

	"entgo.io/ent/dialect/sql"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/cachekey"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/cacheentry"
	entpredicate "github.com/MxOrbit/GitHubActionCacheServer/internal/ent/predicate"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/storagelocation"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/response"
	"github.com/gin-gonic/gin"
)

const (
	defaultManagementItemsPerPage = 20
	maxManagementItemsPerPage     = 100
	managementDeletionBatchSize   = 256
)

type cacheEntryListResponse struct {
	Total int               `json:"total"`
	Items []*ent.CacheEntry `json:"items"`
}

type cacheEntryMatchResponse struct {
	Match *ent.CacheEntry `json:"match"`
	Type  string          `json:"type"`
}

func (h *Handler) ListCacheEntries(c *gin.Context) {
	filters := cacheEntryFilters(c)
	page := positiveQueryInt(c, "page", 1, 0)
	itemsPerPage := positiveQueryInt(c, "itemsPerPage", defaultManagementItemsPerPage, maxManagementItemsPerPage)

	result, err := h.listManagementCacheEntries(c, filters, page, itemsPerPage)
	if err != nil {
		response.InternalError(c, err)
		return
	}

	c.JSON(http.StatusOK, result)
}

func (h *Handler) DeleteCacheEntries(c *gin.Context) {
	if err := h.deleteManagementCacheEntries(c.Request.Context(), cacheEntryFilters(c)); err != nil {
		response.InternalError(c, err)
		return
	}

	c.Status(http.StatusNoContent)
}

func (h *Handler) MatchCacheEntry(c *gin.Context) {
	primaryKey := c.Query("primaryKey")
	version := c.Query("version")
	repoID := c.Query("repoId")
	scopes := c.QueryArray("scopes")
	restoreKeys := nonBlankStrings(c.QueryArray("restoreKeys"))
	if primaryKey == "" || version == "" || repoID == "" || len(scopes) == 0 {
		response.JSON(c, response.Error(http.StatusBadRequest, "missing required query parameters"))
		return
	}

	match, matchType, err := h.matchManagementCacheEntry(c, primaryKey, restoreKeys, version, scopes, repoID)
	if err != nil {
		response.InternalError(c, err)
		return
	}
	if match == nil {
		c.JSON(http.StatusOK, nil)
		return
	}

	c.JSON(http.StatusOK, cacheEntryMatchResponse{Match: match, Type: matchType})
}

func (h *Handler) GetCacheEntry(c *gin.Context) {
	entry, err := h.db.CacheEntry.Query().
		Where(cacheentry.ID(c.Param("id"))).
		Only(c.Request.Context())
	if err != nil {
		if ent.IsNotFound(err) {
			response.JSON(c, response.Error(http.StatusNotFound, "cache entry not found"))
			return
		}
		response.InternalError(c, err)
		return
	}

	c.JSON(http.StatusOK, entry)
}

func (h *Handler) DeleteCacheEntry(c *gin.Context) {
	if err := h.deleteManagementCacheEntry(c, c.Param("id")); err != nil {
		response.InternalError(c, err)
		return
	}

	c.Status(http.StatusNoContent)
}

func (h *Handler) GetStorageLocation(c *gin.Context) {
	location, err := h.db.StorageLocation.Query().
		Where(
			storagelocation.ID(c.Param("id")),
			storagelocation.DeletionRequestedAtIsNil(),
		).
		Only(c.Request.Context())
	if err != nil {
		if ent.IsNotFound(err) {
			response.JSON(c, response.Error(http.StatusNotFound, "storage location not found"))
			return
		}
		response.InternalError(c, err)
		return
	}

	c.JSON(http.StatusOK, location)
}

func (h *Handler) DeleteStorageLocation(c *gin.Context) {
	if err := h.deleteManagementStorageLocation(c, c.Param("id")); err != nil {
		response.InternalError(c, err)
		return
	}

	c.Status(http.StatusNoContent)
}

func (h *Handler) matchManagementCacheEntry(c *gin.Context, primaryKey string, restoreKeys []string, version string, scopes []string, repoID string) (*ent.CacheEntry, string, error) {
	for _, scope := range scopes {
		exactPrimary, err := h.findManagementCacheEntry(c, primaryKey, version, scope, repoID, false)
		if err != nil || exactPrimary != nil {
			return exactPrimary, "exact-primary", err
		}

		prefixedPrimary, err := h.findManagementCacheEntry(c, primaryKey, version, scope, repoID, true)
		if err != nil || prefixedPrimary != nil {
			return prefixedPrimary, "prefixed-primary", err
		}

		for _, restoreKey := range restoreKeys {
			exactRestore, err := h.findManagementCacheEntry(c, restoreKey, version, scope, repoID, false)
			if err != nil || exactRestore != nil {
				return exactRestore, "exact-restore", err
			}

			prefixedRestore, err := h.findManagementCacheEntry(c, restoreKey, version, scope, repoID, true)
			if err != nil || prefixedRestore != nil {
				return prefixedRestore, "prefixed-restore", err
			}
		}
	}
	return nil, "", nil
}

func (h *Handler) findManagementCacheEntry(c *gin.Context, key string, version string, scope string, repoID string, prefix bool) (*ent.CacheEntry, error) {
	query := h.db.CacheEntry.Query().
		Where(
			cacheentry.Version(version),
			cacheentry.Scope(scope),
			cacheentry.RepoId(repoID),
		)
	if prefix {
		query = query.Where(cachekey.Prefix(key)).Order(cacheentry.ByUpdatedAt(sql.OrderDesc()))
	} else {
		query = query.Where(cachekey.Exact(key))
	}

	entry, err := query.First(c.Request.Context())
	if err != nil {
		if ent.IsNotFound(err) {
			return nil, nil
		}
		return nil, err
	}
	return entry, nil
}

func (h *Handler) listManagementCacheEntries(c *gin.Context, filters []entpredicate.CacheEntry, page int, itemsPerPage int) (cacheEntryListResponse, error) {
	query := h.db.CacheEntry.Query().
		Where(filters...).
		Order(cacheentry.ByUpdatedAt(sql.OrderDesc())).
		Limit(itemsPerPage).
		Offset((page - 1) * itemsPerPage)
	items, err := query.All(c.Request.Context())
	if err != nil {
		return cacheEntryListResponse{}, err
	}

	total, err := h.db.CacheEntry.Query().Where(filters...).Count(c.Request.Context())
	if err != nil {
		return cacheEntryListResponse{}, err
	}

	return cacheEntryListResponse{Total: total, Items: items}, nil
}

func (h *Handler) deleteManagementCacheEntry(c *gin.Context, id string) error {
	return h.deleteManagementCacheEntries(c.Request.Context(), []entpredicate.CacheEntry{cacheentry.ID(id)})
}

func (h *Handler) deleteManagementStorageLocation(c *gin.Context, id string) error {
	_, err := h.lifecycle.RequestLocationDeletion(c.Request.Context(), id, true, false)
	return err
}

// deleteManagementCacheEntries deletes matching entries in independently
// committed batches until a transaction observes no remaining matches. Entries
// recreated concurrently can extend the operation. If a later batch fails,
// earlier batches remain committed and the caller receives an error; retrying
// the same predicates is safe.
func (h *Handler) deleteManagementCacheEntries(ctx context.Context, predicates []entpredicate.CacheEntry) error {
	for {
		deleted, err := h.deleteManagementCacheEntryBatch(ctx, predicates)
		if err != nil {
			return err
		}
		if deleted == 0 {
			return nil
		}
	}
}

func (h *Handler) deleteManagementCacheEntryBatch(ctx context.Context, predicates []entpredicate.CacheEntry) (int, error) {
	tx, err := h.db.Tx(ctx)
	if err != nil {
		return 0, fmt.Errorf("start management cache deletion transaction: %w", err)
	}
	committed := false
	defer func() {
		if !committed {
			if rollbackErr := tx.Rollback(); rollbackErr != nil && !errors.Is(rollbackErr, stdsql.ErrTxDone) {
				event := h.logger.Error()
				if ctx.Err() != nil {
					event = h.logger.Debug()
				}
				event.Err(rollbackErr).Msg("management cache deletion rollback failed")
			}
		}
	}()

	entries, err := tx.CacheEntry.Query().
		Where(predicates...).
		Select(cacheentry.FieldID, cacheentry.FieldLocationId).
		Limit(managementDeletionBatchSize).
		All(ctx)
	if err != nil {
		return 0, fmt.Errorf("query management cache entries for deletion: %w", err)
	}
	if len(entries) == 0 {
		if err := tx.Commit(); err != nil {
			return 0, fmt.Errorf("commit empty management cache deletion: %w", err)
		}
		committed = true
		return 0, nil
	}
	if _, err := tx.CacheEntry.Delete().Where(cacheEntryIDs(entries)...).Exec(ctx); err != nil {
		return 0, fmt.Errorf("delete management cache entries: %w", err)
	}
	for _, locationID := range cacheEntryLocationIDs(entries) {
		if _, err := h.lifecycle.FenceDetachedLocation(ctx, tx.Client(), locationID); err != nil {
			return 0, err
		}
	}
	if err := tx.Commit(); err != nil {
		return 0, fmt.Errorf("commit management cache deletion: %w", err)
	}
	committed = true
	return len(entries), nil
}

func cacheEntryFilters(c *gin.Context) []entpredicate.CacheEntry {
	var filters []entpredicate.CacheEntry
	if value := c.Query("key"); value != "" {
		filters = append(filters, cachekey.Exact(value))
	}
	if value := c.Query("version"); value != "" {
		filters = append(filters, cacheentry.Version(value))
	}
	if value := c.Query("scope"); value != "" {
		filters = append(filters, cacheentry.Scope(value))
	}
	if value := c.Query("repoId"); value != "" {
		filters = append(filters, cacheentry.RepoId(value))
	}
	return filters
}

func positiveQueryInt(c *gin.Context, key string, fallback int, max int) int {
	value, err := strconv.Atoi(c.Query(key))
	if err != nil || value < 1 {
		return fallback
	}
	if max > 0 && value > max {
		return max
	}
	return value
}

func cacheEntryLocationIDs(entries []*ent.CacheEntry) []string {
	ids := make([]string, 0, len(entries))
	seen := map[string]struct{}{}
	for _, entry := range entries {
		if entry.LocationId == "" {
			continue
		}
		if _, ok := seen[entry.LocationId]; ok {
			continue
		}
		seen[entry.LocationId] = struct{}{}
		ids = append(ids, entry.LocationId)
	}
	return ids
}

func cacheEntryIDs(entries []*ent.CacheEntry) []entpredicate.CacheEntry {
	ids := make([]string, 0, len(entries))
	for _, entry := range entries {
		ids = append(ids, entry.ID)
	}
	return []entpredicate.CacheEntry{cacheentry.IDIn(ids...)}
}
