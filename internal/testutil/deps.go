package testutil

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/config"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/db"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
	"github.com/stretchr/testify/require"
)

func NewSQLiteClient(t testing.TB) (context.Context, *ent.Client) {
	t.Helper()

	ctx := context.Background()
	client, err := db.OpenAndMigrate(ctx, config.DBConfig{
		Driver:     db.DriverSQLite,
		SQLitePath: filepath.Join(t.TempDir(), "cache.db"),
	})
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, client.Close())
	})

	return ctx, client
}

func NewFilesystemAdapter(t testing.TB) *storage.FilesystemAdapter {
	t.Helper()

	filesystem, err := storage.NewFilesystemAdapter(t.TempDir())
	require.NoError(t, err)
	return filesystem
}

func NewSQLiteFilesystem(t testing.TB) (context.Context, *ent.Client, *storage.FilesystemAdapter) {
	t.Helper()

	ctx, client := NewSQLiteClient(t)
	return ctx, client, NewFilesystemAdapter(t)
}
