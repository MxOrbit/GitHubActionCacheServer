package storageoutbox

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestProcessRetainsFailedDeletionAndCanRetry(t *testing.T) {
	ctx, client, filesystem := testutil.NewSQLiteFilesystem(t)
	require.NoError(t, filesystem.UploadStream(ctx, "folder/parts/0", bytes.NewBufferString("data")))
	task, err := Enqueue(ctx, client, "folder")
	require.NoError(t, err)
	adapter := &failOnceDeleteStorage{Adapter: filesystem}

	err = Process(ctx, client, adapter, task)
	require.ErrorIs(t, err, errInjectedDeleteFailure)
	current := client.StorageDeletion.GetX(ctx, task.ID)
	require.Equal(t, 1, current.AttemptCount)
	require.NotNil(t, current.LastAttemptedAt)
	require.NotNil(t, current.LastError)

	require.NoError(t, Process(ctx, client, adapter, current))
	require.Zero(t, client.StorageDeletion.Query().CountX(ctx))
	count, err := filesystem.CountFilesInFolder(ctx, "folder/parts")
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestEnqueueParticipatesInCallerTransaction(t *testing.T) {
	ctx, client := testutil.NewSQLiteClient(t)
	tx, err := client.Tx(ctx)
	require.NoError(t, err)
	_, err = Enqueue(ctx, tx.Client(), "folder")
	require.NoError(t, err)
	require.NoError(t, tx.Rollback())

	require.Zero(t, client.StorageDeletion.Query().CountX(ctx))
}

var errInjectedDeleteFailure = errors.New("injected delete failure")

type failOnceDeleteStorage struct {
	storage.Adapter
	failed bool
}

func (s *failOnceDeleteStorage) DeleteFolder(ctx context.Context, folderName string) error {
	if !s.failed {
		s.failed = true
		return errInjectedDeleteFailure
	}
	return s.Adapter.DeleteFolder(ctx, folderName)
}
