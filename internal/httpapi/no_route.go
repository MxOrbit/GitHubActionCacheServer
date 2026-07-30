package httpapi

import (
	"net/http"
	"strings"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/managementauth"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/middleware"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/response"
	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
)

func noRoute(logger zerolog.Logger, cfg config.Config) gin.HandlerFunc {
	proxy := fallbackProxy(logger, cfg.Server.DefaultActionsResultsURL)

	return func(c *gin.Context) {
		if isManagementPath(c.Request.URL.Path) {
			writeManagementNoRoute(c, cfg.Management.APIKey)
			return
		}
		proxy(c)
	}
}

func isManagementPath(path string) bool {
	return path == "/management-api" || strings.HasPrefix(path, "/management-api/")
}

func writeManagementNoRoute(c *gin.Context, apiKey string) {
	middleware.SetManagementCORSHeaders(c.Writer.Header())
	if c.Request.Method == http.MethodOptions {
		c.Status(http.StatusNoContent)
		return
	}
	if apiKey == "" {
		response.JSON(c, response.Error(http.StatusServiceUnavailable, "management api is disabled"))
		return
	}
	if !managementauth.Matches(apiKey, c.GetHeader("x-api-key")) {
		response.JSON(c, response.Error(http.StatusUnauthorized, "unauthorized"))
		return
	}
	response.JSON(c, response.Error(http.StatusNotFound, "management endpoint not found"))
}
