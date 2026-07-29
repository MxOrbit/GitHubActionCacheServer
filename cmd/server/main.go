package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/cache"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/cleanup"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/db"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/httpapi"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/metrics"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storagecapacity"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storagelifecycle"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storagereconcile"
	"github.com/rs/zerolog"
)

const shutdownTimeout = 60 * time.Second

func main() {
	cfg, err := config.Load()
	logLevel := zerolog.InfoLevel
	if cfg.Debug {
		logLevel = zerolog.DebugLevel
	}
	logger := zerolog.New(os.Stdout).Level(logLevel).With().Timestamp().Logger()
	if err != nil {
		logger.Fatal().Err(err).Msg("configuration load failed")
	}

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
	metricsRegistry := metrics.New(dbClient)
	lifecycleService := storagelifecycle.New(dbClient)
	capacityService := storagecapacity.NewService(storagecapacity.Options{
		DB:        dbClient,
		Storage:   storageAdapter,
		Config:    cfg.Cache,
		Lifecycle: lifecycleService,
		Logger:    &logger,
	})
	cacheService := cache.NewService(cache.Options{
		DB:                    dbClient,
		Storage:               storageAdapter,
		EnableDirectDownloads: cfg.Cache.EnableDirectDownloads,
		MergeConcurrency:      cfg.Cache.MergeConcurrency,
		Lifecycle:             lifecycleService,
		Logger:                &logger,
		Capacity:              capacityService,
	})

	cleanupCtx, cleanupCancel := context.WithCancel(context.Background())
	defer cleanupCancel()
	storageSizeReady := make(chan struct{})
	go storagereconcile.Run(cleanupCtx, storagereconcile.Options{
		DB:        dbClient,
		Storage:   storageAdapter,
		Lifecycle: lifecycleService,
		Logger:    &logger,
	}, storageSizeReady)
	go capacityService.Run(cleanupCtx, storageSizeReady)
	cleanup.NewRunner(cleanup.NewService(cleanup.Options{
		DB:        dbClient,
		Storage:   storageAdapter,
		Config:    cfg.Cleanup,
		Lifecycle: lifecycleService,
		Logger:    &logger,
		Metrics:   metricsRegistry,
	}), logger, storageSizeReady).Start(cleanupCtx)

	server := &http.Server{
		Addr: cfg.Server.Addr,
		Handler: httpapi.NewRouter(logger, cfg, httpapi.Dependencies{
			DB:        dbClient,
			Storage:   storageAdapter,
			Cache:     cacheService,
			Lifecycle: lifecycleService,
			Metrics:   metricsRegistry,
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
	cacheService.StopAcceptingMerges()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
	defer cancel()

	shutdownErrCh := make(chan error, 1)
	go func() {
		shutdownErrCh <- server.Shutdown(shutdownCtx)
	}()

	mergeErrCh := make(chan error, 1)
	go func() {
		mergeErrCh <- cacheService.WaitForMerges(shutdownCtx)
	}()

	shutdownErr := <-shutdownErrCh
	mergeErr := <-mergeErrCh
	if shutdownErr != nil {
		logger.Fatal().Err(shutdownErr).Msg("graceful shutdown failed")
	}
	if mergeErr != nil {
		logger.Fatal().Err(mergeErr).Msg("waiting for in-flight merges failed")
	}

	if serveErr := <-errCh; serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		logger.Fatal().Err(serveErr).Msg("server stopped with error")
	}

	logger.Info().Msg("server stopped")
}
