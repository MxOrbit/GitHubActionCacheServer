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
