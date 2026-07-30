package cache

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
)

type partsReadCloser struct {
	ctx       context.Context
	storage   storage.Adapter
	folder    string
	partCount int

	mu      sync.Mutex
	index   int
	current io.ReadCloser
	closed  bool
}

func newPartsReadCloser(ctx context.Context, storage storage.Adapter, folder string, partCount int) *partsReadCloser {
	return &partsReadCloser{
		ctx:       ctx,
		storage:   storage,
		folder:    folder,
		partCount: partCount,
	}
}

func (r *partsReadCloser) Read(p []byte) (int, error) {
	for {
		stream, err := r.currentStream()
		if err != nil {
			return 0, err
		}

		n, err := stream.Read(p)
		if err == io.EOF {
			closeErr := r.finishPart(stream)
			if n > 0 {
				return n, nil
			}
			if closeErr != nil {
				return 0, closeErr
			}
			continue
		}
		return n, err
	}
}

func (r *partsReadCloser) Close() error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil
	}
	r.closed = true
	stream := r.current
	r.current = nil
	r.mu.Unlock()

	if stream != nil {
		return stream.Close()
	}
	return nil
}

func (r *partsReadCloser) currentStream() (io.ReadCloser, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, io.ErrClosedPipe
	}
	if r.current != nil {
		stream := r.current
		r.mu.Unlock()
		return stream, nil
	}
	if r.index >= r.partCount {
		r.mu.Unlock()
		return nil, io.EOF
	}
	partIndex := r.index
	r.mu.Unlock()

	stream, err := r.storage.CreateDownloadStream(r.ctx, partObjectName(r.folder, partIndex))
	if err != nil {
		if errors.Is(err, storage.ErrObjectNotFound) {
			return nil, ErrCacheNotFound
		}
		return nil, err
	}

	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return nil, errors.Join(io.ErrClosedPipe, stream.Close())
	}
	r.current = stream
	r.index++
	r.mu.Unlock()
	return stream, nil
}

func (r *partsReadCloser) finishPart(stream io.ReadCloser) error {
	r.mu.Lock()
	if r.current != stream {
		r.mu.Unlock()
		return nil
	}
	r.current = nil
	r.mu.Unlock()
	return stream.Close()
}
