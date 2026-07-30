package handler

import (
	"strings"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/baseurl"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/response"
	twirppb "github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/twirp"
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
	Key       string `json:"key"`
	SizeBytes int64  `json:"size_bytes"`
	Version   string `json:"version"`
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
	body, scope, wireFormat, ok := bindCacheRequest(c, decodeCreateCacheEntryRequest)
	if !ok {
		return
	}

	upload, err := h.cache.CreateUpload(c.Request.Context(), body.cacheKey(), body.cacheVersion(), scope)
	if err != nil {
		h.writeCacheError(c, err)
		return
	}

	base := baseurl.FromRequest(c.Request, h.cfg.Server.APIBaseURL)
	uploadURL := base + "/devstoreaccount1/upload/" + formatInt64(upload.UploadID)
	writeCacheResponse(
		c,
		wireFormat,
		response.CreateCacheEntry(uploadURL),
		&twirppb.CreateCacheEntryResponse{Ok: true, SignedUploadUrl: uploadURL},
	)
}

func (h *Handler) GetCacheEntryDownloadURL(c *gin.Context) {
	body, scope, wireFormat, ok := bindCacheRequest(c, decodeGetCacheEntryDownloadURLRequest)
	if !ok {
		return
	}

	keys := append([]string{body.cacheKey()}, nonBlankStrings(body.RestoreKeys)...)
	base := baseurl.FromRequest(c.Request, h.cfg.Server.APIBaseURL)
	var fallbackSignErr error
	match, err := h.cache.GetCacheEntryWithDownloadURL(
		c.Request.Context(),
		keys,
		body.cacheVersion(),
		scope,
		func(cacheEntryID string) string {
			var signedURL string
			signedURL, fallbackSignErr = h.downloadSigner.Sign(base+"/download/"+cacheEntryID, cacheEntryID)
			return signedURL
		},
	)
	if err != nil {
		h.writeCacheError(c, err)
		return
	}
	if fallbackSignErr != nil {
		response.InternalError(c, fallbackSignErr)
		return
	}
	h.metrics.RecordCacheRequest(match != nil)
	if match == nil {
		writeCacheResponse(
			c,
			wireFormat,
			response.CacheMiss(),
			&twirppb.GetCacheEntryDownloadURLResponse{Ok: false},
		)
		return
	}

	writeCacheResponse(
		c,
		wireFormat,
		response.GetCacheEntryDownloadURL(match.DownloadURL, match.CacheEntry.Key),
		&twirppb.GetCacheEntryDownloadURLResponse{
			Ok:                true,
			SignedDownloadUrl: match.DownloadURL,
			MatchedKey:        match.CacheEntry.Key,
		},
	)
}

func (h *Handler) FinalizeCacheEntryUpload(c *gin.Context) {
	body, scope, wireFormat, ok := bindCacheRequest(c, decodeFinalizeCacheEntryUploadRequest)
	if !ok {
		return
	}

	uploadID, err := h.cache.CompleteUpload(c.Request.Context(), body.cacheKey(), body.cacheVersion(), scope)
	if err != nil {
		h.writeCacheError(c, err)
		return
	}
	h.metrics.RecordCacheUpload()

	writeCacheResponse(
		c,
		wireFormat,
		response.FinalizeCacheEntryUpload(formatInt64(uploadID)),
		&twirppb.FinalizeCacheEntryUploadResponse{Ok: true, EntryId: uploadID},
	)
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
