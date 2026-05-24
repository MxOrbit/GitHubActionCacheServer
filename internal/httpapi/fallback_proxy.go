package httpapi

import (
	"encoding/json"
	"net/http"
	"net/http/httputil"
	"net/url"

	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
)

func fallbackProxy(logger zerolog.Logger, target string) gin.HandlerFunc {
	targetURL, err := url.Parse(target)
	if err != nil || targetURL.Scheme == "" || targetURL.Host == "" {
		return func(c *gin.Context) {
			c.JSON(http.StatusBadGateway, gin.H{"ok": false, "error": "invalid fallback proxy target"})
		}
	}

	proxy := httputil.NewSingleHostReverseProxy(targetURL)
	director := proxy.Director
	proxy.Director = func(req *http.Request) {
		director(req)
		req.Host = targetURL.Host
	}
	proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
		logger.Error().
			Err(err).
			Str("method", req.Method).
			Str("path", req.URL.Path).
			Msg("fallback proxy failed")
		writeFallbackProxyError(rw, http.StatusBadGateway, err.Error())
	}

	return func(c *gin.Context) {
		logger.Debug().
			Str("method", c.Request.Method).
			Str("path", c.Request.URL.RequestURI()).
			Str("target", targetURL.String()).
			Msg("proxying unknown path")
		proxy.ServeHTTP(proxyResponseWriter{ResponseWriter: c.Writer}, c.Request)
	}
}

type proxyResponseWriter struct {
	http.ResponseWriter
}

func (w proxyResponseWriter) Flush() {
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func writeFallbackProxyError(rw http.ResponseWriter, status int, message string) {
	rw.Header().Set("Content-Type", "application/json; charset=utf-8")
	rw.WriteHeader(status)
	_ = json.NewEncoder(rw).Encode(struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
	}{
		OK:    false,
		Error: message,
	})
}
