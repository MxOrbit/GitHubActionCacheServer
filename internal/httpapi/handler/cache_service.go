package handler

import (
	"net/http"
	"strings"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/auth"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/baseurl"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/middleware"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/response"
	"github.com/gin-gonic/gin"
)

type createCacheEntryRequest struct {
	Key     string `json:"key"`
	Version string `json:"version"`
}

type getCacheEntryDownloadURLRequest struct {
	Key         string   `json:"key"`
	RestoreKeys []string `json:"restore_keys"`
	Version     string   `json:"version"`
}

type finalizeCacheEntryUploadRequest struct {
	Key     string `json:"key"`
	Version string `json:"version"`
}

type keyedCacheRequest interface {
	cacheKey() string
	cacheVersion() string
}

func (r createCacheEntryRequest) cacheKey() string {
	return r.Key
}

func (r createCacheEntryRequest) cacheVersion() string {
	return r.Version
}

func (r getCacheEntryDownloadURLRequest) cacheKey() string {
	return r.Key
}

func (r getCacheEntryDownloadURLRequest) cacheVersion() string {
	return r.Version
}

func (r finalizeCacheEntryUploadRequest) cacheKey() string {
	return r.Key
}

func (r finalizeCacheEntryUploadRequest) cacheVersion() string {
	return r.Version
}

func (h *Handler) CreateCacheEntry(c *gin.Context) {
	body, scope, ok := bindCacheRequest[createCacheEntryRequest](c)
	if !ok {
		return
	}

	upload, err := h.cache.CreateUpload(c.Request.Context(), body.cacheKey(), body.cacheVersion(), scope)
	if err != nil {
		writeCacheError(c, err)
		return
	}

	base := baseurl.FromRequest(c.Request, h.cfg.Server.APIBaseURL)
	response.JSON(c, response.CreateCacheEntry(base+"/devstoreaccount1/upload/"+formatInt64(upload.UploadID)))
}

func (h *Handler) GetCacheEntryDownloadURL(c *gin.Context) {
	body, scope, ok := bindCacheRequest[getCacheEntryDownloadURLRequest](c)
	if !ok {
		return
	}

	keys := append([]string{body.cacheKey()}, nonBlankStrings(body.RestoreKeys)...)
	base := baseurl.FromRequest(c.Request, h.cfg.Server.APIBaseURL)
	match, err := h.cache.GetCacheEntryWithDownloadURL(
		c.Request.Context(),
		keys,
		body.cacheVersion(),
		scope,
		func(cacheEntryID string) string {
			return h.downloadSigner.Sign(base+"/download/"+cacheEntryID, cacheEntryID)
		},
	)
	if err != nil {
		writeCacheError(c, err)
		return
	}
	if match == nil {
		response.JSON(c, response.CacheMiss())
		return
	}

	response.JSON(c, response.GetCacheEntryDownloadURL(match.DownloadURL, match.CacheEntry.Key))
}

func (h *Handler) FinalizeCacheEntryUpload(c *gin.Context) {
	body, scope, ok := bindCacheRequest[finalizeCacheEntryUploadRequest](c)
	if !ok {
		return
	}

	uploadID, err := h.cache.CompleteUpload(c.Request.Context(), body.cacheKey(), body.cacheVersion(), scope)
	if err != nil {
		writeCacheError(c, err)
		return
	}

	response.JSON(c, response.FinalizeCacheEntryUpload(formatInt64(uploadID)))
}

func bindCacheRequest[T keyedCacheRequest](c *gin.Context) (T, auth.CacheScope, bool) {
	var body T
	if err := c.ShouldBindJSON(&body); err != nil || body.cacheKey() == "" || body.cacheVersion() == "" {
		response.JSON(c, response.Error(http.StatusBadRequest, "invalid body"))
		return body, auth.CacheScope{}, false
	}

	scope, ok := middleware.CacheScope(c)
	if !ok {
		response.JSON(c, response.Error(http.StatusUnauthorized, "cache scope missing"))
		return body, auth.CacheScope{}, false
	}

	return body, scope, true
}

func nonBlankStrings(values []string) []string {
	filtered := make([]string, 0, len(values))
	for _, value := range values {
		if strings.TrimSpace(value) == "" {
			continue
		}
		filtered = append(filtered, value)
	}
	return filtered
}
