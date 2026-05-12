package middleware

import (
	"net/http"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/auth"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/response"
	"github.com/gin-gonic/gin"
)

const cacheScopeKey = "cache_scope"

func RequireCacheScope(verifier *auth.Verifier) gin.HandlerFunc {
	return func(c *gin.Context) {
		scope, err := verifier.CacheScope(c.Request.Context(), c.GetHeader("Authorization"))
		if err != nil {
			response.JSON(c, response.Error(http.StatusUnauthorized, err.Error()))
			c.Abort()
			return
		}

		c.Set(cacheScopeKey, scope)
		c.Next()
	}
}

func CacheScope(c *gin.Context) (auth.CacheScope, bool) {
	value, ok := c.Get(cacheScopeKey)
	if !ok {
		return auth.CacheScope{}, false
	}

	scope, ok := value.(auth.CacheScope)
	return scope, ok
}
