package main

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestHTTPServerTimeoutsDoNotLimitCacheTransfers(t *testing.T) {
	handler := http.HandlerFunc(func(http.ResponseWriter, *http.Request) {})
	server := newHTTPServer(":3000", handler)

	require.Equal(t, serverReadHeaderTimeout, server.ReadHeaderTimeout)
	require.Equal(t, serverIdleTimeout, server.IdleTimeout)
	require.Zero(t, server.ReadTimeout)
	require.Zero(t, server.WriteTimeout)
}
