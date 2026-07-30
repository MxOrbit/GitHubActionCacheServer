package middleware

import (
	"net/http"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/managementauth"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/response"
	"github.com/gin-gonic/gin"
)

func ManagementCORS() gin.HandlerFunc {
	return func(c *gin.Context) {
		SetManagementCORSHeaders(c.Writer.Header())
		if c.Request.Method == http.MethodOptions {
			c.AbortWithStatus(http.StatusNoContent)
			return
		}

		c.Next()
	}
}

func SetManagementCORSHeaders(headers http.Header) {
	headers.Set("Access-Control-Allow-Origin", "*")
	headers.Set("Access-Control-Allow-Methods", "GET,POST,DELETE,OPTIONS")
	headers.Set("Access-Control-Allow-Headers", "Content-Type,Authorization,X-Api-Key,x-api-key")
	headers.Set("Access-Control-Expose-Headers", "Content-Type")
	headers.Set("Access-Control-Max-Age", "600")
}

func RequireManagementAPIKey(apiKey string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if apiKey == "" {
			response.JSON(c, response.Error(http.StatusServiceUnavailable, "management api is disabled"))
			c.Abort()
			return
		}
		if !managementauth.Matches(apiKey, c.GetHeader("x-api-key")) {
			response.JSON(c, response.Error(http.StatusUnauthorized, "unauthorized"))
			c.Abort()
			return
		}

		c.Next()
	}
}
