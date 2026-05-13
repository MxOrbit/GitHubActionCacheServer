package cache

import (
	"context"
	"errors"
	"io"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
)

type partsReadCloser struct {
	ctx       context.Context
	storage   storage.Adapter
	folder    string
	partCount int

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
	if r.closed {
		return 0, io.ErrClosedPipe
	}

	for {
		if r.current == nil {
			if r.index >= r.partCount {
				return 0, io.EOF
			}

			stream, err := r.storage.CreateDownloadStream(r.ctx, partObjectName(r.folder, r.index))
			if err != nil {
				if errors.Is(err, storage.ErrObjectNotFound) {
					return 0, ErrCacheNotFound
				}
				return 0, err
			}
			r.current = stream
			r.index++
		}

		n, err := r.current.Read(p)
		if err == io.EOF {
			closeErr := r.current.Close()
			r.current = nil
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
	r.closed = true
	if r.current != nil {
		err := r.current.Close()
		r.current = nil
		return err
	}
	return nil
}
