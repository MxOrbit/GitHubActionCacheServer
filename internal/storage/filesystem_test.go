package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestFilesystemAdapterUploadDownloadCountAndDelete(t *testing.T) {
	ctx := context.Background()
	adapter, err := NewFilesystemAdapter(t.TempDir())
	require.NoError(t, err)

	require.NoError(t, adapter.UploadStream(ctx, "folder/parts/0", strings.NewReader("hello ")))
	require.NoError(t, adapter.UploadStream(ctx, "folder/parts/1", strings.NewReader("world")))

	count, err := adapter.CountFilesInFolder(ctx, "folder/parts")
	require.NoError(t, err)
	require.Equal(t, 2, count)

	stream, err := adapter.CreateDownloadStream(ctx, "folder/parts/0")
	require.NoError(t, err)

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.Equal(t, "hello ", string(body))
	require.NoError(t, stream.Close())

	require.NoError(t, adapter.DeleteFolder(ctx, "folder/parts"))
	count, err = adapter.CountFilesInFolder(ctx, "folder/parts")
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestFilesystemInventorySeparatesLogicalRepresentationsFromPhysicalBytes(t *testing.T) {
	ctx := context.Background()
	adapter, err := NewFilesystemAdapter(t.TempDir())
	require.NoError(t, err)

	require.NoError(t, adapter.UploadStream(ctx, "folder/parts/0", strings.NewReader("hello ")))
	require.NoError(t, adapter.UploadStream(ctx, "folder/parts/1", strings.NewReader("world")))
	require.NoError(t, adapter.UploadStream(ctx, "folder/parts/3", strings.NewReader("stale")))
	require.NoError(t, adapter.UploadStream(ctx, "folder/blocks/YmxvY2s", strings.NewReader("duplicate")))
	require.NoError(t, adapter.UploadStream(ctx, "folder/merged", strings.NewReader("hello world")))
	require.NoError(t, adapter.UploadStream(ctx, "folder/unknown/object", strings.NewReader("unknown")))
	require.NoError(t, adapter.UploadStream(ctx, "loose", strings.NewReader("root")))

	inventory, err := adapter.Inventory(ctx)
	require.NoError(t, err)
	require.Equal(t, int64(7), inventory.ObjectCount)
	require.Equal(t, int64(47), inventory.PhysicalBytes)
	require.Len(t, inventory.LooseObjects, 1)

	folder, ok := inventory.Folder("folder")
	require.True(t, ok)
	require.Equal(t, int64(6), folder.ObjectCount)
	require.Equal(t, int64(43), folder.PhysicalBytes)
	require.Len(t, folder.Parts, 3)
	require.Equal(t, int64(9), folder.Blocks.PhysicalBytes)
	require.Equal(t, int64(7), folder.Unknown.PhysicalBytes)
	require.NotNil(t, folder.Merged)
	require.Equal(t, int64(11), folder.Merged.SizeBytes)

	logicalSize, err := folder.LogicalPartsSize(2)
	require.NoError(t, err)
	require.Equal(t, int64(11), logicalSize)
	_, err = folder.LogicalPartsSize(3)
	require.ErrorIs(t, err, ErrIndexedObjectMissing)

	parts, err := adapter.InspectFolder(ctx, "folder/parts")
	require.NoError(t, err)
	logicalSize, err = parts.LogicalIndexedSize(2)
	require.NoError(t, err)
	require.Equal(t, int64(11), logicalSize)

	metadata, err := adapter.InspectObject(ctx, "folder/merged")
	require.NoError(t, err)
	require.Equal(t, int64(11), metadata.SizeBytes)
	require.False(t, metadata.ModifiedAt.IsZero())
}

func TestFilesystemInventoryCollectsCompleteTemporaryUploadNames(t *testing.T) {
	ctx := context.Background()
	adapter, err := NewFilesystemAdapter(t.TempDir())
	require.NoError(t, err)

	partsPath := filepath.Join(adapter.root, "location", "parts")
	require.NoError(t, os.MkdirAll(partsPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(partsPath, filesystemUploadTempPrefix+"stale"), []byte("temporary"), 0o644))
	require.NoError(t, os.WriteFile(filepath.Join(partsPath, "0"), []byte("committed"), 0o644))

	inventory, err := adapter.Inventory(ctx)
	require.NoError(t, err)
	require.Len(t, inventory.TemporaryUploads, 1)
	require.Equal(t, "location/parts/"+filesystemUploadTempPrefix+"stale", inventory.TemporaryUploads[0].Name)
	require.Equal(t, int64(len("temporary")), inventory.TemporaryUploads[0].SizeBytes)
	require.False(t, inventory.TemporaryUploads[0].ModifiedAt.IsZero())
	require.Equal(t, int64(2), inventory.ObjectCount)
	require.Equal(t, int64(len("temporary")+len("committed")), inventory.PhysicalBytes)
}

func TestFilesystemCleanupTemporaryUploadsRechecksCandidatesAndContinuesAfterFailure(t *testing.T) {
	ctx := context.Background()
	adapter, err := NewFilesystemAdapter(t.TempDir())
	require.NoError(t, err)

	partsPath := filepath.Join(adapter.root, "location", "parts")
	require.NoError(t, os.MkdirAll(partsPath, 0o755))
	oldPath := filepath.Join(partsPath, filesystemUploadTempPrefix+"old")
	recentPath := filepath.Join(partsPath, filesystemUploadTempPrefix+"recent")
	vanishedPath := filepath.Join(partsPath, filesystemUploadTempPrefix+"vanished")
	committedPath := filepath.Join(partsPath, "0")
	for _, objectPath := range []string{oldPath, recentPath, vanishedPath, committedPath} {
		require.NoError(t, os.WriteFile(objectPath, []byte("data"), 0o644))
	}

	now := time.Now().Truncate(time.Second)
	old := now.Add(-25 * time.Hour)
	require.NoError(t, os.Chtimes(oldPath, old, old))
	require.NoError(t, os.Chtimes(vanishedPath, old, old))

	inventory, err := adapter.Inventory(ctx)
	require.NoError(t, err)
	require.Len(t, inventory.TemporaryUploads, 3)
	require.NoError(t, os.Remove(vanishedPath))

	candidates := append([]ObjectMetadata{{
		Name:       "../" + filesystemUploadTempPrefix + "invalid",
		ModifiedAt: old,
	}}, inventory.TemporaryUploads...)
	deleted, err := adapter.CleanupTemporaryUploads(ctx, candidates, now.Add(-24*time.Hour))
	require.ErrorContains(t, err, "resolve temporary upload")
	require.Equal(t, 1, deleted)
	require.NoFileExists(t, oldPath)
	require.FileExists(t, recentPath)
	require.NoFileExists(t, vanishedPath)
	require.FileExists(t, committedPath)
}

func TestFilesystemCleanupTemporaryUploadsToleratesConcurrentRename(t *testing.T) {
	ctx := context.Background()
	adapter, err := NewFilesystemAdapter(t.TempDir())
	require.NoError(t, err)

	partsPath := filepath.Join(adapter.root, "location", "parts")
	require.NoError(t, os.MkdirAll(partsPath, 0o755))
	temporaryPath := filepath.Join(partsPath, filesystemUploadTempPrefix+"committing")
	committedPath := filepath.Join(partsPath, "0")
	require.NoError(t, os.WriteFile(temporaryPath, []byte("data"), 0o644))
	old := time.Now().Add(-25 * time.Hour)
	require.NoError(t, os.Chtimes(temporaryPath, old, old))

	inventory, err := adapter.Inventory(ctx)
	require.NoError(t, err)
	require.Len(t, inventory.TemporaryUploads, 1)
	require.NoError(t, os.Rename(temporaryPath, committedPath))

	deleted, err := adapter.CleanupTemporaryUploads(ctx, inventory.TemporaryUploads, time.Now().Add(-24*time.Hour))
	require.NoError(t, err)
	require.Zero(t, deleted)
	require.FileExists(t, committedPath)
}

func TestFilesystemCleanupTemporaryUploadsToleratesConcurrentRemoval(t *testing.T) {
	ctx := context.Background()
	adapter, err := NewFilesystemAdapter(t.TempDir())
	require.NoError(t, err)

	partsPath := filepath.Join(adapter.root, "location", "parts")
	require.NoError(t, os.MkdirAll(partsPath, 0o755))
	temporaryPath := filepath.Join(partsPath, filesystemUploadTempPrefix+"removed")
	require.NoError(t, os.WriteFile(temporaryPath, []byte("data"), 0o644))
	old := time.Now().Add(-25 * time.Hour)
	require.NoError(t, os.Chtimes(temporaryPath, old, old))

	inventory, err := adapter.Inventory(ctx)
	require.NoError(t, err)
	require.Len(t, inventory.TemporaryUploads, 1)
	adapter.removeTemporaryUpload = func(objectPath string) error {
		require.NoError(t, os.Remove(objectPath))
		return os.ErrNotExist
	}

	deleted, err := adapter.CleanupTemporaryUploads(ctx, inventory.TemporaryUploads, time.Now().Add(-24*time.Hour))
	require.NoError(t, err)
	require.Zero(t, deleted)
	require.NoFileExists(t, temporaryPath)
}

func TestFilesystemFolderMetadataIncludesNestedDirectoryActivity(t *testing.T) {
	ctx := context.Background()
	adapter, err := NewFilesystemAdapter(t.TempDir())
	require.NoError(t, err)

	folderPath := filepath.Join(adapter.root, "folder")
	blocksPath := filepath.Join(folderPath, "blocks")
	require.NoError(t, os.MkdirAll(blocksPath, 0o755))
	objectPath := filepath.Join(blocksPath, "old-block")
	require.NoError(t, os.WriteFile(objectPath, []byte("data"), 0o644))

	old := time.Now().Add(-48 * time.Hour).Truncate(time.Second)
	recent := time.Now().Add(-time.Hour).Truncate(time.Second)
	require.NoError(t, os.Chtimes(objectPath, old, old))
	require.NoError(t, os.Chtimes(folderPath, old, old))
	require.NoError(t, os.Chtimes(blocksPath, recent, recent))

	contents, err := adapter.InspectFolder(ctx, "folder")
	require.NoError(t, err)
	require.True(t, contents.Exists)
	require.WithinDuration(t, recent, contents.NewestModifiedAt, time.Second)

	inventory, err := adapter.Inventory(ctx)
	require.NoError(t, err)
	folder, ok := inventory.Folder("folder")
	require.True(t, ok)
	require.WithinDuration(t, recent, folder.NewestModifiedAt, time.Second)
}

func TestFilesystemFolderMetadataDistinguishesEmptyAndMissingFolders(t *testing.T) {
	ctx := context.Background()
	adapter, err := NewFilesystemAdapter(t.TempDir())
	require.NoError(t, err)

	emptyPath := filepath.Join(adapter.root, "empty")
	require.NoError(t, os.MkdirAll(emptyPath, 0o755))
	modifiedAt := time.Now().Add(-time.Hour).Truncate(time.Second)
	require.NoError(t, os.Chtimes(emptyPath, modifiedAt, modifiedAt))

	empty, err := adapter.InspectFolder(ctx, "empty")
	require.NoError(t, err)
	require.True(t, empty.Exists)
	require.Zero(t, empty.ObjectCount)
	require.WithinDuration(t, modifiedAt, empty.NewestModifiedAt, time.Second)

	missing, err := adapter.InspectFolder(ctx, "missing")
	require.NoError(t, err)
	require.False(t, missing.Exists)
	require.True(t, missing.NewestModifiedAt.IsZero())

	inventory, err := adapter.Inventory(ctx)
	require.NoError(t, err)
	emptyFolder, ok := inventory.Folder("empty")
	require.True(t, ok)
	require.WithinDuration(t, modifiedAt, emptyFolder.NewestModifiedAt, time.Second)
}

func TestLogicalIndexedSizeRejectsZeroPartLegacyLocation(t *testing.T) {
	_, err := (FolderContents{}).LogicalIndexedSize(0)
	require.Error(t, err)
	require.NotErrorIs(t, err, ErrIndexedObjectMissing)
}

func TestFilesystemAdapterRejectsPathTraversal(t *testing.T) {
	ctx := context.Background()
	adapter, err := NewFilesystemAdapter(t.TempDir())
	require.NoError(t, err)

	tests := []string{
		"../escape",
		"..",
		"/absolute",
		`..\escape`,
	}

	for _, tt := range tests {
		t.Run(tt, func(t *testing.T) {
			err := adapter.UploadStream(ctx, tt, strings.NewReader("bad"))
			require.Error(t, err)
		})
	}
}

func TestFilesystemAdapterReturnsObjectNotFound(t *testing.T) {
	ctx := context.Background()
	adapter, err := NewFilesystemAdapter(t.TempDir())
	require.NoError(t, err)

	_, err = adapter.CreateDownloadStream(ctx, "missing")
	require.ErrorIs(t, err, ErrObjectNotFound)

	var notFound ObjectNotFoundError
	require.True(t, errors.As(err, &notFound))
	require.Equal(t, "missing", notFound.ObjectName)
}

func TestFilesystemAdapterObjectExists(t *testing.T) {
	ctx := context.Background()
	adapter, err := NewFilesystemAdapter(t.TempDir())
	require.NoError(t, err)
	require.NoError(t, adapter.UploadStream(ctx, "folder/object", strings.NewReader("data")))
	require.NoError(t, os.MkdirAll(filepath.Join(adapter.root, "folder", "directory"), 0o755))

	exists, err := adapter.ObjectExists(ctx, "folder/object")
	require.NoError(t, err)
	require.True(t, exists)

	exists, err = adapter.ObjectExists(ctx, "folder/missing")
	require.NoError(t, err)
	require.False(t, exists)

	exists, err = adapter.ObjectExists(ctx, "folder/directory")
	require.NoError(t, err)
	require.False(t, exists)

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	exists, err = adapter.ObjectExists(canceled, "folder/object")
	require.ErrorIs(t, err, context.Canceled)
	require.False(t, exists)
}

func TestFilesystemAdapterReportsVolumeUsage(t *testing.T) {
	ctx := context.Background()
	adapter, err := NewFilesystemAdapter(t.TempDir())
	require.NoError(t, err)

	usage, err := adapter.FilesystemUsage(ctx)

	require.NoError(t, err)
	require.Positive(t, usage.CapacityBytes)
	require.GreaterOrEqual(t, usage.UsedBytes, int64(0))
	require.LessOrEqual(t, usage.UsedBytes, usage.CapacityBytes)
}

func TestFilesystemAdapterClear(t *testing.T) {
	ctx := context.Background()
	adapter, err := NewFilesystemAdapter(t.TempDir())
	require.NoError(t, err)

	require.NoError(t, adapter.UploadStream(ctx, "folder/object", strings.NewReader("data")))
	require.NoError(t, adapter.Clear(ctx))

	count, err := adapter.CountFilesInFolder(ctx, "folder")
	require.NoError(t, err)
	require.Zero(t, count)
}

func TestFilesystemAdapterRemovesPartialFileAfterFailedUpload(t *testing.T) {
	ctx := context.Background()
	adapter, err := NewFilesystemAdapter(t.TempDir())
	require.NoError(t, err)

	err = adapter.UploadStream(ctx, "folder/parts/0", failingReader{})
	require.Error(t, err)

	count, err := adapter.CountFilesInFolder(ctx, "folder/parts")
	require.NoError(t, err)
	require.Zero(t, count)

	_, err = adapter.CreateDownloadStream(ctx, "folder/parts/0")
	require.ErrorIs(t, err, ErrObjectNotFound)
}

func TestFilesystemAdapterDoesNotPublishObjectAfterFailedSync(t *testing.T) {
	ctx := context.Background()
	adapter, err := NewFilesystemAdapter(t.TempDir())
	require.NoError(t, err)

	syncErr := errors.New("sync failed")
	adapter.syncUpload = func(*os.File) error { return syncErr }

	err = adapter.UploadStream(ctx, "folder/parts/0", strings.NewReader("data"))
	require.ErrorIs(t, err, syncErr)

	entries, err := os.ReadDir(filepath.Join(adapter.root, "folder", "parts"))
	require.NoError(t, err)
	require.Empty(t, entries)

	_, err = adapter.CreateDownloadStream(ctx, "folder/parts/0")
	require.ErrorIs(t, err, ErrObjectNotFound)
}

func TestFilesystemAdapterCanDisableUploadFsync(t *testing.T) {
	adapter, err := newFilesystemAdapter(t.TempDir(), false)
	require.NoError(t, err)
	require.Nil(t, adapter.syncUpload)
	require.NoError(t, adapter.UploadStream(context.Background(), "folder/object", strings.NewReader("data")))
}

func TestFilesystemAdapterRemovesTempFileAfterCanceledUpload(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	adapter, err := NewFilesystemAdapter(t.TempDir())
	require.NoError(t, err)

	err = adapter.UploadStream(ctx, "folder/object", cancelAfterFirstRead{cancel: cancel})
	require.ErrorIs(t, err, context.Canceled)

	entries, err := os.ReadDir(filepath.Join(adapter.root, "folder"))
	require.NoError(t, err)
	require.Empty(t, entries)

	_, err = adapter.CreateDownloadStream(context.Background(), "folder/object")
	require.ErrorIs(t, err, ErrObjectNotFound)
}

func TestFilesystemAdapterCountSkipsTemporaryUploads(t *testing.T) {
	ctx := context.Background()
	adapter, err := NewFilesystemAdapter(t.TempDir())
	require.NoError(t, err)

	partsPath := filepath.Join(adapter.root, "folder", "parts")
	require.NoError(t, os.MkdirAll(partsPath, 0o755))
	require.NoError(t, os.WriteFile(filepath.Join(partsPath, filesystemUploadTempPrefix+"stale"), []byte("partial"), 0o644))
	require.NoError(t, adapter.UploadStream(ctx, "folder/parts/0", strings.NewReader("complete")))

	count, err := adapter.CountFilesInFolder(ctx, "folder/parts")
	require.NoError(t, err)
	require.Equal(t, 1, count)
}

func TestFilesystemAdapterUploadReplacesExistingObject(t *testing.T) {
	ctx := context.Background()
	adapter, err := NewFilesystemAdapter(t.TempDir())
	require.NoError(t, err)

	require.NoError(t, adapter.UploadStream(ctx, "folder/object", strings.NewReader("old")))
	require.NoError(t, adapter.UploadStream(ctx, "folder/object", strings.NewReader("new")))

	stream, err := adapter.CreateDownloadStream(ctx, "folder/object")
	require.NoError(t, err)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	require.Equal(t, "new", string(body))
}

func TestFilesystemAdapterCopyObjectPreservesIndependentObjectSemantics(t *testing.T) {
	ctx := context.Background()
	adapter, err := NewFilesystemAdapter(t.TempDir())
	require.NoError(t, err)

	require.NoError(t, adapter.UploadStream(ctx, "folder/blocks/source", strings.NewReader("old")))
	require.NoError(t, adapter.CopyObject(ctx, "folder/blocks/source", "folder/parts/0"))
	require.Equal(t, "old", readFilesystemObject(t, ctx, adapter, "folder/blocks/source"))
	require.Equal(t, "old", readFilesystemObject(t, ctx, adapter, "folder/parts/0"))

	require.NoError(t, adapter.UploadStream(ctx, "folder/blocks/source", strings.NewReader("new")))
	require.Equal(t, "old", readFilesystemObject(t, ctx, adapter, "folder/parts/0"))

	require.NoError(t, adapter.CopyObject(ctx, "folder/blocks/source", "folder/parts/0"))
	require.Equal(t, "new", readFilesystemObject(t, ctx, adapter, "folder/parts/0"))

	require.NoError(t, adapter.DeleteFolder(ctx, "folder/blocks"))
	require.Equal(t, "new", readFilesystemObject(t, ctx, adapter, "folder/parts/0"))
}

func TestFilesystemAdapterCopyObjectReturnsObjectNotFound(t *testing.T) {
	ctx := context.Background()
	adapter, err := NewFilesystemAdapter(t.TempDir())
	require.NoError(t, err)

	err = adapter.CopyObject(ctx, "folder/blocks/missing", "folder/parts/0")
	require.ErrorIs(t, err, ErrObjectNotFound)

	var notFound ObjectNotFoundError
	require.True(t, errors.As(err, &notFound))
	require.Equal(t, "folder/blocks/missing", notFound.ObjectName)
}

func readFilesystemObject(t *testing.T, ctx context.Context, adapter *FilesystemAdapter, objectName string) string {
	t.Helper()

	stream, err := adapter.CreateDownloadStream(ctx, objectName)
	require.NoError(t, err)
	defer stream.Close()

	body, err := io.ReadAll(stream)
	require.NoError(t, err)
	return string(body)
}

type failingReader struct{}

func (r failingReader) Read(p []byte) (int, error) {
	copy(p, "partial")
	return len("partial"), fmt.Errorf("read failed")
}

type cancelAfterFirstRead struct {
	cancel context.CancelFunc
	done   bool
}

func (r cancelAfterFirstRead) Read(p []byte) (int, error) {
	if r.done {
		return 0, io.EOF
	}
	copy(p, "partial")
	r.cancel()
	return len("partial"), nil
}
