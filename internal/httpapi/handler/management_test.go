package handler

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/ent/cacheentry"
	entpredicate "github.com/MxOrbit/GitHubActionCacheServer/internal/ent/predicate"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/storagelifecycle"
	"github.com/MxOrbit/GitHubActionCacheServer/internal/testutil"
	"github.com/stretchr/testify/require"
)

func TestDeleteManagementCacheEntriesCommitsAndResumesInBatches(t *testing.T) {
	ctx, client := testutil.NewSQLiteClient(t)
	lifecycle := storagelifecycle.New(client)
	handler := New(Options{DB: client, Lifecycle: lifecycle})

	sharedLocation := client.StorageLocation.Create().
		SetID("shared-location").
		SetFolderName("shared-folder").
		SetPartCount(1).
		SaveX(ctx)
	entries := make([]*ent.CacheEntryCreate, 0, managementDeletionBatchSize+1)
	for index := range managementDeletionBatchSize + 1 {
		entries = append(entries, client.CacheEntry.Create().
			SetID(fmt.Sprintf("target-%03d", index)).
			SetKey("target-key").
			SetVersion("version").
			SetScope("scope").
			SetRepoId("target-repo").
			SetUpdatedAt(int64(index)).
			SetLocation(sharedLocation))
	}
	_, err := client.CacheEntry.CreateBulk(entries...).Save(ctx)
	require.NoError(t, err)

	retainedLocation := client.StorageLocation.Create().
		SetID("retained-location").
		SetFolderName("retained-folder").
		SetPartCount(1).
		SaveX(ctx)
	client.CacheEntry.Create().
		SetID("retained-entry").
		SetKey("retained-key").
		SetVersion("version").
		SetScope("scope").
		SetRepoId("retained-repo").
		SetUpdatedAt(0).
		SetLocation(retainedLocation).
		SaveX(ctx)

	injectedErr := errors.New("injected second batch failure")
	deleteCalls := 0
	failSecondBatch := true
	client.CacheEntry.Use(func(next ent.Mutator) ent.Mutator {
		return ent.MutateFunc(func(mutationCtx context.Context, mutation ent.Mutation) (ent.Value, error) {
			if mutation.Op().Is(ent.OpDelete) {
				deleteCalls++
				if failSecondBatch && deleteCalls == 2 {
					return nil, injectedErr
				}
			}
			return next.Mutate(mutationCtx, mutation)
		})
	})

	predicates := []entpredicate.CacheEntry{cacheentry.RepoId("target-repo")}
	err = handler.deleteManagementCacheEntries(ctx, predicates)
	require.ErrorIs(t, err, injectedErr)
	require.Equal(t, 2, deleteCalls)
	require.Equal(t, 1, client.CacheEntry.Query().Where(predicates...).CountX(ctx))
	require.Nil(t, client.StorageLocation.GetX(ctx, sharedLocation.ID).DeletionRequestedAt)
	require.True(t, client.CacheEntry.Query().Where(cacheentry.ID("retained-entry")).ExistX(ctx))

	failSecondBatch = false
	require.NoError(t, handler.deleteManagementCacheEntries(ctx, predicates))
	require.Equal(t, 3, deleteCalls)
	require.Zero(t, client.CacheEntry.Query().Where(predicates...).CountX(ctx))
	require.NotNil(t, client.StorageLocation.GetX(ctx, sharedLocation.ID).DeletionRequestedAt)
	require.True(t, client.CacheEntry.Query().Where(cacheentry.ID("retained-entry")).ExistX(ctx))
}
