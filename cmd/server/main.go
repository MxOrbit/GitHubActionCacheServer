package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/tools"
	"github.com/rs/zerolog"
)

const shutdownTimeout = 60 * time.Second

func main() {
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	server := &http.Server{
		Addr:    tools.EnvOrDefault("ADDR", ":3000"),
		Handler: httpapi.NewRouter(logger),
	}

	errCh := make(chan error, 1)
	go func() {
		logger.Info().Str("addr", server.Addr).Msg("server listening")
		errCh <- server.ListenAndServe()
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	select {
	case <-ctx.Done():
		logger.Info().Dur("timeout", shutdownTimeout).Msg("shutdown signal received")
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return
		}
		logger.Fatal().Err(err).Msg("server failed")
	}

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		logger.Fatal().Err(err).Msg("graceful shutdown failed")
	}

	if err := <-errCh; err != nil && !errors.Is(err, http.ErrServerClosed) {
		logger.Fatal().Err(err).Msg("server stopped with error")
	}

	logger.Info().Msg("server stopped")
}
