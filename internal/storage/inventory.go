package storage

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"time"
)

// MaxIndexedObjects is the Azure block-list protocol limit. Keeping the limit
// here lets storage inspection reject corrupt legacy rows before allocating a
// bitmap derived from an untrusted database value.
const MaxIndexedObjects = 50_000

var (
	ErrIndexedObjectMissing       = errors.New("indexed storage object is missing")
	ErrIndexedObjectLimitExceeded = errors.New("indexed storage object count exceeds supported limit")
)

type IndexedObjectMissingError struct {
	Index int
}

func (e IndexedObjectMissingError) Error() string {
	return fmt.Sprintf("%s: %d", ErrIndexedObjectMissing, e.Index)
}

func (e IndexedObjectMissingError) Unwrap() error {
	return ErrIndexedObjectMissing
}

type IndexedObjectLimitExceededError struct {
	Count int
	Limit int
}

func (e IndexedObjectLimitExceededError) Error() string {
	return fmt.Sprintf("%s: count %d, limit %d", ErrIndexedObjectLimitExceeded, e.Count, e.Limit)
}

func (e IndexedObjectLimitExceededError) Unwrap() error {
	return ErrIndexedObjectLimitExceeded
}

type ObjectMetadata struct {
	Name       string
	SizeBytes  int64
	ModifiedAt time.Time
}

type FolderSummary struct {
	FolderName       string
	Exists           bool
	ObjectCount      int64
	PhysicalBytes    int64
	NewestModifiedAt time.Time
}

type indexedFolderAccumulator struct {
	expected     int
	seen         []uint64
	sizes        []int64
	logicalBytes int64
}

func newIndexedFolderAccumulator(expected int) (*indexedFolderAccumulator, error) {
	if expected < 1 {
		return nil, fmt.Errorf("invalid finalized part count %d", expected)
	}
	if expected > MaxIndexedObjects {
		return nil, IndexedObjectLimitExceededError{Count: expected, Limit: MaxIndexedObjects}
	}
	return &indexedFolderAccumulator{
		expected: expected,
		seen:     make([]uint64, (expected+63)/64),
	}, nil
}

func newIndexedFolderSizeAccumulator(expected int) (*indexedFolderAccumulator, error) {
	accumulator, err := newIndexedFolderAccumulator(expected)
	if err != nil {
		return nil, err
	}
	accumulator.sizes = make([]int64, expected)
	return accumulator, nil
}

func (a *indexedFolderAccumulator) add(relativeName string, sizeBytes int64) error {
	index, err := strconv.Atoi(relativeName)
	if err != nil || index < 0 || index >= a.expected || strconv.Itoa(index) != relativeName {
		return nil
	}
	word, mask := index/64, uint64(1)<<uint(index%64)
	if a.seen[word]&mask != 0 {
		return nil
	}
	a.seen[word] |= mask
	if a.sizes != nil {
		a.sizes[index] = sizeBytes
	}
	a.logicalBytes, err = addStorageBytes(a.logicalBytes, sizeBytes)
	return err
}

func (a *indexedFolderAccumulator) result() (int64, error) {
	for index := range a.expected {
		word, mask := index/64, uint64(1)<<uint(index%64)
		if a.seen[word]&mask == 0 {
			return 0, IndexedObjectMissingError{Index: index}
		}
	}
	return a.logicalBytes, nil
}

func (a *indexedFolderAccumulator) resultSizes() ([]int64, error) {
	if _, err := a.result(); err != nil {
		return nil, err
	}
	return a.sizes, nil
}

func validateObjectMetadata(object ObjectMetadata) error {
	if object.Name == "" {
		return fmt.Errorf("storage contains an object without a name")
	}
	if object.SizeBytes < 0 {
		return fmt.Errorf("storage object %q has negative size %d", object.Name, object.SizeBytes)
	}
	if object.ModifiedAt.IsZero() {
		return fmt.Errorf("storage object %q has no modification time", object.Name)
	}
	return nil
}

func addFolderSummaryObject(summary *FolderSummary, object ObjectMetadata) error {
	if err := validateObjectMetadata(object); err != nil {
		return err
	}
	var err error
	summary.PhysicalBytes, err = addStorageBytes(summary.PhysicalBytes, object.SizeBytes)
	if err != nil {
		return err
	}
	summary.ObjectCount++
	summary.NewestModifiedAt = newestTime(summary.NewestModifiedAt, object.ModifiedAt)
	return nil
}

func addStorageBytes(total, size int64) (int64, error) {
	if size < 0 || total > math.MaxInt64-size {
		return 0, fmt.Errorf("storage byte count overflows int64")
	}
	return total + size, nil
}

func newestTime(current, candidate time.Time) time.Time {
	if candidate.After(current) {
		return candidate
	}
	return current
}

func PartsFolder(folderName string) string {
	return folderName + "/parts"
}

func MergedObject(folderName string) string {
	return folderName + "/merged"
}
