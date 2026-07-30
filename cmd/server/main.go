package main

import (
	"context"
	"errors"
	"net/http"
	"os"
	"os/signal"
	"sync"
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

const (
	shutdownTimeout         = 60 * time.Second
	serverReadHeaderTimeout = 30 * time.Second
	serverIdleTimeout       = 5 * time.Minute
)

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

	backgroundCtx, backgroundCancel := context.WithCancel(context.Background())
	defer backgroundCancel()
	storageSizeReady := make(chan struct{})
	var backgroundServices sync.WaitGroup
	backgroundServices.Go(func() {
		storagereconcile.Run(backgroundCtx, storagereconcile.Options{
			DB:        dbClient,
			Storage:   storageAdapter,
			Lifecycle: lifecycleService,
			Logger:    &logger,
		}, storageSizeReady)
	})
	backgroundServices.Go(func() {
		capacityService.Run(backgroundCtx, storageSizeReady)
	})
	cleanupRunner := cleanup.NewRunner(cleanup.NewService(cleanup.Options{
		DB:        dbClient,
		Storage:   storageAdapter,
		Config:    cfg.Cleanup,
		Lifecycle: lifecycleService,
		Logger:    &logger,
		Metrics:   metricsRegistry,
	}), logger, storageSizeReady)
	backgroundServices.Go(func() {
		cleanupRunner.Run(backgroundCtx)
	})
	backgroundDone := make(chan struct{})
	go func() {
		backgroundServices.Wait()
		close(backgroundDone)
	}()

	server := newHTTPServer(cfg.Server.Addr, httpapi.NewRouter(logger, cfg, httpapi.Dependencies{
		DB:        dbClient,
		Storage:   storageAdapter,
		Cache:     cacheService,
		Lifecycle: lifecycleService,
		Metrics:   metricsRegistry,
	}))

	errCh := make(chan error, 1)
	go func() {
		logger.Info().Str("addr", server.Addr).Msg("server listening")
		errCh <- server.ListenAndServe()
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var serveErr error
	serveResultReceived := false
	select {
	case <-ctx.Done():
		stop()
		logger.Info().Dur("timeout", shutdownTimeout).Msg("shutdown signal received")
	case serveErr = <-errCh:
		serveResultReceived = true
		if !errors.Is(serveErr, http.ErrServerClosed) {
			logger.Fatal().Err(serveErr).Msg("server failed")
		}
	}

	backgroundCancel()
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

	backgroundErrCh := make(chan error, 1)
	go func() {
		backgroundErrCh <- waitForBackgroundServices(shutdownCtx, backgroundDone)
	}()

	shutdownErr := <-shutdownErrCh
	mergeErr := <-mergeErrCh
	backgroundErr := <-backgroundErrCh
	if shutdownErr != nil {
		logger.Fatal().Err(shutdownErr).Msg("graceful shutdown failed")
	}
	if mergeErr != nil {
		logger.Fatal().Err(mergeErr).Msg("waiting for in-flight merges failed")
	}
	if backgroundErr != nil {
		logger.Fatal().Err(backgroundErr).Msg("waiting for background services failed")
	}

	if !serveResultReceived {
		serveErr = <-errCh
	}
	if serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
		logger.Fatal().Err(serveErr).Msg("server stopped with error")
	}

	logger.Info().Msg("server stopped")
}

func newHTTPServer(addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:              addr,
		Handler:           handler,
		ReadHeaderTimeout: serverReadHeaderTimeout,
		IdleTimeout:       serverIdleTimeout,
	}
}

func waitForBackgroundServices(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		select {
		case <-done:
			return nil
		default:
			return ctx.Err()
		}
	}
}
