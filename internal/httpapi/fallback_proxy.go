package httpapi

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/bufferpool"
	"github.com/gin-gonic/gin"
	"github.com/rs/zerolog"
)

const fallbackProxyResponseHeaderTimeout = 60 * time.Second

func fallbackProxy(logger zerolog.Logger, target string) gin.HandlerFunc {
	return fallbackProxyWithResponseHeaderTimeout(logger, target, fallbackProxyResponseHeaderTimeout)
}

func fallbackProxyWithResponseHeaderTimeout(logger zerolog.Logger, target string, responseHeaderTimeout time.Duration) gin.HandlerFunc {
	targetURL, err := url.Parse(target)
	if err != nil || targetURL.Scheme == "" || targetURL.Host == "" {
		if err == nil {
			err = fmt.Errorf("fallback proxy target requires a scheme and host")
		}
		logger.Error().Err(err).Msg("invalid fallback proxy target")
		return func(c *gin.Context) {
			c.JSON(http.StatusBadGateway, gin.H{"ok": false, "error": "bad gateway"})
		}
	}

	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = responseHeaderTimeout
	proxy := &httputil.ReverseProxy{
		BufferPool: bufferpool.Default,
		Transport:  bufferpool.WrapTransport(transport),
		Rewrite: func(req *httputil.ProxyRequest) {
			req.SetURL(targetURL)
			req.SetXForwarded()
		},
	}
	proxy.ErrorHandler = func(rw http.ResponseWriter, req *http.Request, err error) {
		event := logger.Error()
		if isExpectedRequestCancellation(req.Context(), err) {
			event = logger.Debug()
		}
		event.
			Err(err).
			Str("method", req.Method).
			Str("path", req.URL.Path).
			Msg("fallback proxy failed")
		if writeErr := writeFallbackProxyError(rw, http.StatusBadGateway, "bad gateway"); writeErr != nil {
			event := logger.Error()
			if isExpectedRequestCancellation(req.Context(), writeErr) {
				event = logger.Debug()
			}
			event.
				Err(errors.Join(err, writeErr)).
				Str("method", req.Method).
				Str("path", req.URL.Path).
				Msg("fallback proxy error response failed")
		}
	}

	return func(c *gin.Context) {
		logger.Debug().
			Str("method", c.Request.Method).
			Str("path", c.Request.URL.Path).
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

func writeFallbackProxyError(rw http.ResponseWriter, status int, message string) error {
	rw.Header().Set("Content-Type", "application/json; charset=utf-8")
	rw.WriteHeader(status)
	return json.NewEncoder(rw).Encode(struct {
		OK    bool   `json:"ok"`
		Error string `json:"error"`
	}{
		OK:    false,
		Error: message,
	})
}
