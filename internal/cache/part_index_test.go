package cache

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPartIndexUsesPrefixBoundaries(t *testing.T) {
	index, err := newPartIndex([]int64{3, 0, 4, 2})
	require.NoError(t, err)
	require.Equal(t, []int64{3, 3, 7, 9}, index.ends)
	require.Equal(t, int64(9), index.total)

	tests := []struct {
		offset     int64
		part       int
		partOffset int64
	}{
		{offset: 0, part: 0, partOffset: 0},
		{offset: 2, part: 0, partOffset: 2},
		{offset: 3, part: 2, partOffset: 0},
		{offset: 6, part: 2, partOffset: 3},
		{offset: 7, part: 3, partOffset: 0},
		{offset: 9, part: 4, partOffset: 0},
	}
	for _, test := range tests {
		part, partOffset := index.locate(test.offset)
		require.Equal(t, test.part, part)
		require.Equal(t, test.partOffset, partOffset)
	}
}

func TestPartIndexRejectsInvalidSizes(t *testing.T) {
	_, err := newPartIndex([]int64{1, -1})
	require.ErrorContains(t, err, "part 1")
	_, err = newPartIndex([]int64{1<<63 - 1, 1})
	require.ErrorContains(t, err, "part 1")
}

func TestPartIndexCacheReusesSuccessfulLoad(t *testing.T) {
	cache := newPartIndexCache(1024)
	key := partIndexKey{locationID: "location", folderName: "folder", partCount: 2}
	var calls atomic.Int32
	load := func(context.Context) (*partIndex, error) {
		calls.Add(1)
		return newPartIndex([]int64{2, 3})
	}

	first, delayed, err := cache.getOrLoad(context.Background(), key, load)
	require.NoError(t, err)
	require.True(t, delayed)
	second, delayed, err := cache.getOrLoad(context.Background(), key, load)
	require.NoError(t, err)
	require.False(t, delayed)
	require.Same(t, first, second)
	require.Equal(t, int32(1), calls.Load())
}

func TestPartIndexCacheCanceledWaiterDoesNotCancelLoader(t *testing.T) {
	cache := newPartIndexCache(1024)
	key := partIndexKey{locationID: "location", folderName: "folder", partCount: 2}
	started := make(chan struct{})
	release := make(chan struct{})
	leaderDone := make(chan error, 1)
	go func() {
		_, _, err := cache.getOrLoad(context.Background(), key, func(context.Context) (*partIndex, error) {
			close(started)
			<-release
			return newPartIndex([]int64{2, 3})
		})
		leaderDone <- err
	}()
	<-started

	waiterCtx, cancel := context.WithCancel(context.Background())
	cancel()
	_, delayed, err := cache.getOrLoad(waiterCtx, key, func(context.Context) (*partIndex, error) {
		return nil, errors.New("waiter unexpectedly became loader")
	})
	require.ErrorIs(t, err, context.Canceled)
	require.True(t, delayed)

	close(release)
	require.NoError(t, <-leaderDone)
	index, delayed, err := cache.getOrLoad(context.Background(), key, nil)
	require.NoError(t, err)
	require.False(t, delayed)
	require.Equal(t, int64(5), index.total)
}

func TestPartIndexCacheEvictsByByteCapacity(t *testing.T) {
	firstKey := partIndexKey{locationID: "first", folderName: "folder", partCount: 2}
	secondKey := partIndexKey{locationID: "second", folderName: "folder", partCount: 2}
	first, err := newPartIndex([]int64{1, 1})
	require.NoError(t, err)
	second, err := newPartIndex([]int64{2, 2})
	require.NoError(t, err)
	oneEntryCapacity := partIndexEntryOverhead + int64(len(secondKey.locationID)+len(secondKey.folderName)) + 16
	cache := newPartIndexCache(oneEntryCapacity)

	cache.add(firstKey, first)
	cache.add(secondKey, second)

	cache.mu.Lock()
	defer cache.mu.Unlock()
	require.Len(t, cache.entries, 1)
	require.Nil(t, cache.entries[firstKey])
	require.NotNil(t, cache.entries[secondKey])
	require.LessOrEqual(t, cache.used, cache.capacity)
}
