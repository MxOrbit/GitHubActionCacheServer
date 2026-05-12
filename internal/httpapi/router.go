package httpapi

import (
	"net/http"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/auth"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/handler"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/middleware"
	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
)

func NewRouter(logger zerolog.Logger) http.Handler {
	gin.SetMode(gin.ReleaseMode)

	router := gin.New()
	router.Use(requestLogger(logger), gin.Recovery())

	router.GET("/", handler.Root)
	router.GET("/health", handler.Health)

	cacheService := router.Group(
		"/twirp/github.actions.results.api.v1.CacheService",
		middleware.RequireCacheScope(auth.NewFromEnv()),
	)
	cacheService.POST("/CreateCacheEntry", handler.CreateCacheEntry)
	cacheService.POST("/GetCacheEntryDownloadURL", handler.GetCacheEntryDownloadURL)
	cacheService.POST("/FinalizeCacheEntryUpload", handler.FinalizeCacheEntryUpload)

	router.PUT("/devstoreaccount1/upload/:uploadId", handler.UploadPart)
	router.PUT("/upload/:uploadId", handler.UploadPart)
	router.GET("/download/:cacheEntryId", handler.DownloadCacheEntry)

	management := router.Group("/management-api")
	{
		cacheEntries := management.Group("/cache-entries")
		cacheEntries.GET("/", handler.ListCacheEntries)
		cacheEntries.DELETE("/", handler.DeleteCacheEntries)
		cacheEntries.GET("/match", handler.MatchCacheEntry)
		cacheEntries.GET("/:id", handler.GetCacheEntry)
		cacheEntries.DELETE("/:id", handler.DeleteCacheEntry)

		storageLocations := management.Group("/storage-locations")
		storageLocations.GET("/:id", handler.GetStorageLocation)
		storageLocations.DELETE("/:id", handler.DeleteStorageLocation)
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
