package cache

import (
	"context"
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
	"github.com/stretchr/testify/require"
)

func TestPartsReadCloserCanCloseWhileReading(t *testing.T) {
	stream := newBlockingPartStream()
	adapter := &partsReaderStorage{
		createDownloadStream: func(context.Context, string) (io.ReadCloser, error) {
			return stream, nil
		},
	}
	reader := newPartsReadCloser(context.Background(), adapter, "folder", 1)

	readDone := make(chan error, 1)
	go func() {
		_, err := reader.Read(make([]byte, 1))
		readDone <- err
	}()
	<-stream.readStarted

	require.NoError(t, reader.Close())
	require.ErrorIs(t, <-readDone, io.ErrClosedPipe)
	_, err := reader.Read(make([]byte, 1))
	require.ErrorIs(t, err, io.ErrClosedPipe)
	require.NoError(t, reader.Close())
}

func TestPartsReadCloserClosesStreamOpenedDuringClose(t *testing.T) {
	stream := newBlockingPartStream()
	openStarted := make(chan struct{})
	releaseOpen := make(chan struct{})
	adapter := &partsReaderStorage{
		createDownloadStream: func(context.Context, string) (io.ReadCloser, error) {
			close(openStarted)
			<-releaseOpen
			return stream, nil
		},
	}
	reader := newPartsReadCloser(context.Background(), adapter, "folder", 1)

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

type partsReaderStorage struct {
	storage.Adapter
	createDownloadStream func(context.Context, string) (io.ReadCloser, error)
}

func (s *partsReaderStorage) CreateDownloadStream(ctx context.Context, objectName string) (io.ReadCloser, error) {
	return s.createDownloadStream(ctx, objectName)
}

type blockingPartStream struct {
	readStarted chan struct{}
	closed      chan struct{}
	readOnce    sync.Once
	closeOnce   sync.Once
}

func newBlockingPartStream() *blockingPartStream {
	return &blockingPartStream{
		readStarted: make(chan struct{}),
		closed:      make(chan struct{}),
	}
}

func (s *blockingPartStream) Read([]byte) (int, error) {
	s.readOnce.Do(func() { close(s.readStarted) })
	<-s.closed
	return 0, io.ErrClosedPipe
}

func (s *blockingPartStream) Close() error {
	s.closeOnce.Do(func() { close(s.closed) })
	return nil
}

func (s *blockingPartStream) readError() error {
	select {
	case <-s.closed:
		return io.ErrClosedPipe
	default:
		return errors.New("stream is still open")
	}
}
