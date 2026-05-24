package httpapi

import (
	"net/http"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/auth"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/cache"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/handler"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/middleware"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
)

type Dependencies struct {
	DB      *ent.Client
	Storage storage.Adapter
}

func NewRouter(logger zerolog.Logger, cfg config.Config, deps Dependencies) http.Handler {
	gin.SetMode(gin.ReleaseMode)

	router := gin.New()
	router.Use(requestLogger(logger), gin.Recovery())

	handlers := handler.New(handler.Options{
		Config: cfg,
		Cache: cache.NewService(cache.Options{
			DB:                    deps.DB,
			Storage:               deps.Storage,
			EnableDirectDownloads: cfg.Cache.EnableDirectDownloads,
		}),
		DB:      deps.DB,
		Storage: deps.Storage,
	})

	router.GET("/", handler.Root)
	router.GET("/health", handler.Health)

	cacheService := router.Group(
		"/twirp/github.actions.results.api.v1.CacheService",
		middleware.RequireCacheScope(auth.NewVerifier(auth.Options{
			Issuer:         cfg.Auth.TokenIssuer,
			JWKSURL:        cfg.Auth.TokenJWKSURL,
			SkipValidation: cfg.Auth.SkipTokenValidation,
		})),
	)
	cacheService.POST("/CreateCacheEntry", handlers.CreateCacheEntry)
	cacheService.POST("/GetCacheEntryDownloadURL", handlers.GetCacheEntryDownloadURL)
	cacheService.POST("/FinalizeCacheEntryUpload", handlers.FinalizeCacheEntryUpload)

	router.PUT("/devstoreaccount1/upload/:uploadId", handlers.UploadPart)
	router.PUT("/upload/:uploadId", handlers.UploadPart)
	router.GET("/download/:cacheEntryId", handlers.DownloadCacheEntry)

	management := router.Group("/management-api", middleware.RequireManagementAPIKey(cfg.Management.APIKey))
	{
		cacheEntries := management.Group("/cache-entries")
		cacheEntries.GET("/", handlers.ListCacheEntries)
		cacheEntries.DELETE("/", handlers.DeleteCacheEntries)
		cacheEntries.GET("/match", handlers.MatchCacheEntry)
		cacheEntries.GET("/:id", handlers.GetCacheEntry)
		cacheEntries.DELETE("/:id", handlers.DeleteCacheEntry)

		storageLocations := management.Group("/storage-locations")
		storageLocations.GET("/:id", handlers.GetStorageLocation)
		storageLocations.DELETE("/:id", handlers.DeleteStorageLocation)
	}

	return router
}

func requestLogger(logger zerolog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		start := time.Now()
		c.Next()

		logger.Info().
			Str("method", c.Request.Method).
			Str("path", c.Request.URL.Path).
			Int("status", c.Writer.Status()).
			Dur("duration", time.Since(start)).
			Msg("http request")
	}
}
