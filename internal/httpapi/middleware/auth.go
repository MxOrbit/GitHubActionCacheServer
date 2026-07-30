package middleware

import (
	"errors"
	"net/http"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/auth"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi/response"
	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
)

const cacheScopeKey = "cache_scope"

func RequireCacheScope(verifier *auth.Verifier, logger zerolog.Logger) gin.HandlerFunc {
	return func(c *gin.Context) {
		scope, err := verifier.CacheScope(c.Request.Context(), c.GetHeader("Authorization"))
		if err != nil {
			if errors.Is(err, auth.ErrVerifierInitialization) {
				response.RecordInternalError(c, err)
			} else {
				logger.Debug().
					Err(err).
					Str("method", c.Request.Method).
					Str("path", c.Request.URL.Path).
					Msg("cache authentication rejected")
			}
			response.JSON(c, response.Error(http.StatusUnauthorized, "unauthorized"))
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
