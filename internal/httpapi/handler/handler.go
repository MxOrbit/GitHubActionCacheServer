package handler

import (
	"errors"
	"net/http"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/cache"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/downloadurl"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/response"
	"github.com/gin-gonic/gin"
)

const fallbackDownloadURLTTL = 10 * time.Minute

type Handler struct {
	cfg            config.Config
	cache          *cache.Service
	downloadSigner *downloadurl.Signer
}

type Options struct {
	Config config.Config
	Cache  *cache.Service
}

func New(options Options) *Handler {
	return &Handler{
		cfg:            options.Config,
		cache:          options.Cache,
		downloadSigner: downloadurl.New(options.Config.Cache.DownloadURLSigningSecret, fallbackDownloadURLTTL),
	}
}

func writeCacheError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, cache.ErrNoWriteScope):
		response.JSON(c, response.Error(http.StatusForbidden, err.Error()))
	case errors.Is(err, cache.ErrUploadAlreadyExists):
		response.JSON(c, response.CacheMiss())
	case errors.Is(err, cache.ErrUploadNotFound), errors.Is(err, cache.ErrCacheNotFound):
		response.JSON(c, response.Error(http.StatusNotFound, err.Error()))
	case errors.Is(err, cache.ErrNoPartsUploaded), errors.Is(err, cache.ErrPartsStillUploading), errors.Is(err, cache.ErrPartCountMismatch):
		response.JSON(c, response.Error(http.StatusBadRequest, err.Error()))
	default:
		response.JSON(c, response.Error(http.StatusInternalServerError, err.Error()))
	}
}
