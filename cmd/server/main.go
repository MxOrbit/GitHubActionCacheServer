package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/cleanup"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/db"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
	"github.com/rs/zerolog"
)

const shutdownTimeout = 60 * time.Second

func main() {
	cfg := config.Load()
	logger := zerolog.New(os.Stdout).With().Timestamp().Logger()

	dbClient, err := db.OpenAndMigrate(context.Background(), cfg.DB)
	if err != nil {
		logger.Fatal().Err(err).Msg("database initialization failed")
	}
	defer func() {
		if err := dbClient.Close(); err != nil {
			logger.Error().Err(err).Msg("database close failed")
		}
	}()

	storageAdapter, err := storage.NewAdapter(context.Background(), cfg.Storage)
	if err != nil {
		logger.Fatal().Err(err).Msg("storage initialization failed")
	}

	cleanupCtx, cleanupCancel := context.WithCancel(context.Background())
	defer cleanupCancel()
	cleanup.NewRunner(cleanup.NewService(cleanup.Options{
		DB:      dbClient,
		Storage: storageAdapter,
		Config:  cfg.Cleanup,
	}), logger).Start(cleanupCtx)

	server := &http.Server{
		Addr: cfg.Server.Addr,
		Handler: httpapi.NewRouter(logger, cfg, httpapi.Dependencies{
			DB:      dbClient,
			Storage: storageAdapter,
		}),
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
	case serveErr := <-errCh:
		if errors.Is(serveErr, http.ErrServerClosed) {
			return
		}
		logger.Fatal().Err(serveErr).Msg("server failed")
	}

	cleanupCancel()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	if shutdownErr := server.Shutdown(shutdownCtx); shutdownErr != nil {
		logger.Fatal().Err(shutdownErr).Msg("graceful shutdown failed")
	}

	if serveErr := <-errCh; serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		logger.Fatal().Err(serveErr).Msg("server stopped with error")
	}

	logger.Info().Msg("server stopped")
}
