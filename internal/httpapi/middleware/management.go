package middleware

import (
	"net/http"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/response"
	"github.com/gin-gonic/gin"
)

func RequireManagementAPIKey(apiKey string) gin.HandlerFunc {
	return func(c *gin.Context) {
		if apiKey == "" {
			response.JSON(c, response.Error(http.StatusServiceUnavailable, "management api is disabled"))
			c.Abort()
			return
		}
		if c.GetHeader("x-api-key") != apiKey {
			response.JSON(c, response.Error(http.StatusUnauthorized, "unauthorized"))
			c.Abort()
			return
		}

		c.Next()
	}
}
