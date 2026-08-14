package cache

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestRangedPartsReadCloserCanCloseWhileReading(t *testing.T) {
	stream := newBlockingPartStream()
	adapter := &rangedPartsReaderStorage{
		createRangedDownloadStream: func(context.Context, string, int64, int64) (io.ReadCloser, error) {
			return stream, nil
		},
	}
	index, err := newPartIndex([]int64{1})
	require.NoError(t, err)
	reader := newRangedPartsReadCloser(context.Background(), adapter, "folder", index, DownloadRange{Count: 1})

	readDone := make(chan error, 1)
	go func() {
		_, err := reader.Read(make([]byte, 1))
		readDone <- err
	}()
	<-stream.readStarted

	require.NoError(t, reader.Close())
	require.ErrorIs(t, <-readDone, io.ErrClosedPipe)
	_, err = reader.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.ErrClosedPipe)
	require.NoError(t, reader.Close())
}

func TestRangedPartsReadCloserClosesStreamOpenedDuringClose(t *testing.T) {
	stream := newBlockingPartStream()
	openStarted := make(chan struct{})
	releaseOpen := make(chan struct{})
	adapter := &rangedPartsReaderStorage{
		createRangedDownloadStream: func(context.Context, string, int64, int64) (io.ReadCloser, error) {
			close(openStarted)
			<-releaseOpen
			return stream, nil
		},
	}
	index, err := newPartIndex([]int64{1})
	require.NoError(t, err)
	reader := newRangedPartsReadCloser(context.Background(), adapter, "folder", index, DownloadRange{Count: 1})

	readDone := make(chan error, 1)
	go func() {
		_, err := reader.Read(make([]byte, 1))
		readDone <- err
	}()
	<-openStarted

	require.NoError(t, reader.Close())
	close(releaseOpen)
	require.ErrorIs(t, <-readDone, io.ErrClosedPipe)
	require.ErrorIs(t, stream.readError(), io.ErrClosedPipe)
}

func TestRangedPartsReadCloserSkipsEmptyPartInsideRange(t *testing.T) {
	var opened []string
	adapter := &rangedPartsReaderStorage{
		createRangedDownloadStream: func(_ context.Context, objectName string, offset, count int64) (io.ReadCloser, error) {
			opened = append(opened, objectName)
			contents := map[string]string{
				"folder/parts/0": "ab",
				"folder/parts/2": "cd",
			}[objectName]
			return io.NopCloser(strings.NewReader(contents[offset : offset+count])), nil
		},
	}
	index, err := newPartIndex([]int64{2, 0, 2})
	require.NoError(t, err)
	reader := newRangedPartsReadCloser(context.Background(), adapter, "folder", index, DownloadRange{Count: 4})

	body, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, reader.Close())
	require.Equal(t, "abcd", string(body))
	require.Equal(t, []string{"folder/parts/0", "folder/parts/2"}, opened)
}

type rangedPartsReaderStorage struct {
	storage.Adapter
	createRangedDownloadStream func(context.Context, string, int64, int64) (io.ReadCloser, error)
}

func (s *rangedPartsReaderStorage) CreateRangedDownloadStream(ctx context.Context, objectName string, offset, count int64) (io.ReadCloser, error) {
	return s.createRangedDownloadStream(ctx, objectName, offset, count)
}
