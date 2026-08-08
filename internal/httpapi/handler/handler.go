package handler

import (
	"errors"
	"net/http"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/cache"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/downloadurl"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/response"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/metrics"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storagelifecycle"
	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
)

const (
	fallbackDownloadURLTTL = 10 * time.Minute
	uploadURLTTL           = 24 * time.Hour
)

type Handler struct {
	cfg            config.Config
	cache          *cache.Service
	db             *ent.Client
	storage        storage.Adapter
	lifecycle      *storagelifecycle.Service
	downloadSigner *downloadurl.Signer
	uploadSigner   *downloadurl.Signer
	metrics        metrics.Recorder
	logger         zerolog.Logger
}

type Options struct {
	Config    config.Config
	Cache     *cache.Service
	DB        *ent.Client
	Storage   storage.Adapter
	Lifecycle *storagelifecycle.Service
	Metrics   metrics.Recorder
	Logger    *zerolog.Logger
}

func New(options Options) *Handler {
	lifecycle := options.Lifecycle
	if lifecycle == nil {
		lifecycle = storagelifecycle.New(options.DB)
	}
	logger := zerolog.Nop()
	if options.Logger != nil {
		logger = *options.Logger
	}
	metricsRecorder := options.Metrics
	if metricsRecorder == nil {
		metricsRecorder = metrics.NopRecorder()
	}
	return &Handler{
		cfg:            options.Config,
		cache:          options.Cache,
		db:             options.DB,
		storage:        options.Storage,
		lifecycle:      lifecycle,
		downloadSigner: downloadurl.New(options.Config.Cache.DownloadURLSigningSecret, fallbackDownloadURLTTL),
		uploadSigner:   downloadurl.New(options.Config.Cache.DownloadURLSigningSecret, uploadURLTTL),
		metrics:        metricsRecorder,
		logger:         logger,
	}
}

func (h *Handler) writeCacheError(c *gin.Context, err error) {
	switch {
	case errors.Is(err, cache.ErrNoWriteScope):
		h.logExpectedCacheError(c, err)
		response.JSON(c, response.Error(http.StatusForbidden, cache.ErrNoWriteScope.Error()))
	case errors.Is(err, cache.ErrUploadAlreadyExists):
		h.logExpectedCacheError(c, err)
		response.JSON(c, response.CacheMiss())
	case errors.Is(err, cache.ErrUploadNotFound), errors.Is(err, cache.ErrCacheNotFound):
		h.logExpectedCacheError(c, err)
		message := cache.ErrCacheNotFound.Error()
		if errors.Is(err, cache.ErrUploadNotFound) {
			message = cache.ErrUploadNotFound.Error()
		}
		response.JSON(c, response.Error(http.StatusNotFound, message))
	case errors.Is(err, cache.ErrNoPartsUploaded), errors.Is(err, cache.ErrPartCountMismatch), errors.Is(err, cache.ErrBlockListTooLarge):
		h.logExpectedCacheError(c, err)
		message := cache.ErrNoPartsUploaded.Error()
		switch {
		case errors.Is(err, cache.ErrPartCountMismatch):
			message = cache.ErrPartCountMismatch.Error()
		case errors.Is(err, cache.ErrBlockListTooLarge):
			message = cache.ErrBlockListTooLarge.Error()
		}
		response.JSON(c, response.Error(http.StatusBadRequest, message))
	default:
		response.InternalError(c, err)
	}
}

func (h *Handler) logExpectedCacheError(c *gin.Context, err error) {
	h.logger.Debug().
		Err(err).
		Str("method", c.Request.Method).
		Str("path", c.Request.URL.Path).
		Msg("cache request rejected")
}
