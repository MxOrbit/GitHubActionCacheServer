package main

import (
	"context"
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

func TestWaitForBackgroundServicesPrefersCompletionWhenContextIsAlsoCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	done := make(chan struct{})
	close(done)

	require.NoError(t, waitForBackgroundServices(ctx, done))
}

func TestWaitForBackgroundServicesReturnsContextErrorWhileStillRunning(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	require.ErrorIs(t, waitForBackgroundServices(ctx, make(chan struct{})), context.Canceled)
}
