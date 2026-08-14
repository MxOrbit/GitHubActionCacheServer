package cache

import (
	"container/list"
	"context"
	"fmt"
	"math"
	"sort"
	"sync"
)

const partIndexEntryOverhead = int64(64)

type partIndex struct {
	ends  []int64
	total int64
}

func newPartIndex(sizes []int64) (*partIndex, error) {
	index := &partIndex{ends: make([]int64, len(sizes))}
	for part, size := range sizes {
		if size < 0 || index.total > math.MaxInt64-size {
			return nil, fmt.Errorf("invalid ranged cache part %d size %d", part, size)
		}
		index.total += size
		index.ends[part] = index.total
	}
	return index, nil
}

func (i *partIndex) locate(offset int64) (int, int64) {
	part := sort.Search(len(i.ends), func(part int) bool { return i.ends[part] > offset })
	if part == len(i.ends) {
		return part, 0
	}
	start := int64(0)
	if part > 0 {
		start = i.ends[part-1]
	}
	return part, offset - start
}

func (i *partIndex) size(part int) int64 {
	start := int64(0)
	if part > 0 {
		start = i.ends[part-1]
	}
	return i.ends[part] - start
}

type partIndexKey struct {
	locationID string
	folderName string
	partCount  int
}

type partIndexCacheEntry struct {
	key    partIndexKey
	index  *partIndex
	weight int64
}

type partIndexFlight struct {
	done chan struct{}
}

// partIndexCache keeps immutable finalized-part boundaries. Its capacity is
// charged by bytes rather than entry count because one location may contain up
// to storage.MaxIndexedObjects parts. In-flight loads are serialized per
// location; a waiter whose own context is canceled stops waiting without
// canceling the loader.
type partIndexCache struct {
	mu       sync.Mutex
	capacity int64
	used     int64
	lru      list.List
	entries  map[partIndexKey]*list.Element
	flights  map[partIndexKey]*partIndexFlight
}

func newPartIndexCache(capacity int64) *partIndexCache {
	return &partIndexCache{
		capacity: capacity,
		entries:  make(map[partIndexKey]*list.Element),
		flights:  make(map[partIndexKey]*partIndexFlight),
	}
}

func (c *partIndexCache) getOrLoad(ctx context.Context, key partIndexKey, load func(context.Context) (*partIndex, error)) (*partIndex, bool, error) {
	waited := false
	for {
		c.mu.Lock()
		if element := c.entries[key]; element != nil {
			c.lru.MoveToFront(element)
			index := element.Value.(*partIndexCacheEntry).index
			c.mu.Unlock()
			return index, waited, nil
		}
		if flight := c.flights[key]; flight != nil {
			done := flight.done
			c.mu.Unlock()
			select {
			case <-ctx.Done():
				return nil, true, ctx.Err()
			case <-done:
				waited = true
				continue
			}
		}

		flight := &partIndexFlight{done: make(chan struct{})}
		c.flights[key] = flight
		c.mu.Unlock()

		index, err := load(ctx)
		c.mu.Lock()
		delete(c.flights, key)
		if err == nil {
			c.addLocked(key, index)
		}
		close(flight.done)
		c.mu.Unlock()
		return index, true, err
	}
}

func (c *partIndexCache) add(key partIndexKey, index *partIndex) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.addLocked(key, index)
}

func (c *partIndexCache) addLocked(key partIndexKey, index *partIndex) {
	if index == nil || c.capacity <= 0 {
		return
	}
	if element := c.entries[key]; element != nil {
		c.lru.MoveToFront(element)
		return
	}
	weight := partIndexEntryOverhead + int64(len(key.locationID)+len(key.folderName)) + int64(len(index.ends))*8
	if weight > c.capacity {
		return
	}
	for c.used+weight > c.capacity {
		oldest := c.lru.Back()
		if oldest == nil {
			break
		}
		entry := oldest.Value.(*partIndexCacheEntry)
		delete(c.entries, entry.key)
		c.used -= entry.weight
		c.lru.Remove(oldest)
	}
	entry := &partIndexCacheEntry{key: key, index: index, weight: weight}
	c.entries[key] = c.lru.PushFront(entry)
	c.used += weight
}
