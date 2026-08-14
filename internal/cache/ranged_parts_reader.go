package cache

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/storage"
)

type rangedPartsReadCloser struct {
	ctx     context.Context
	storage storage.Adapter
	folder  string
	parts   *partIndex

	mu           sync.Mutex
	index        int
	partOffset   int64
	remaining    int64
	current      io.ReadCloser
	currentCount int64
	closed       bool
}

func newRangedPartsReadCloser(ctx context.Context, adapter storage.Adapter, folder string, parts *partIndex, selected DownloadRange) *rangedPartsReadCloser {
	index, partOffset := parts.locate(selected.Offset)
	return &rangedPartsReadCloser{
		ctx:        ctx,
		storage:    adapter,
		folder:     folder,
		parts:      parts,
		index:      index,
		partOffset: partOffset,
		remaining:  selected.Count,
	}
}

func (r *rangedPartsReadCloser) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	for {
		stream, err := r.currentStream()
		if err != nil {
			return 0, err
		}
		n, err := stream.Read(p)
		if errors.Is(err, io.EOF) {
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

func (r *rangedPartsReadCloser) Close() error {
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

func (r *rangedPartsReadCloser) currentStream() (io.ReadCloser, error) {
	for {
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
		if r.remaining == 0 {
			r.mu.Unlock()
			return nil, io.EOF
		}
		if r.index >= len(r.parts.ends) {
			r.mu.Unlock()
			return nil, io.ErrUnexpectedEOF
		}
		partIndex := r.index
		partOffset := r.partOffset
		available := r.parts.size(partIndex) - partOffset
		if available <= 0 {
			r.index++
			r.partOffset = 0
			r.mu.Unlock()
			continue
		}
		count := min(available, r.remaining)
		r.mu.Unlock()

		stream, err := r.storage.CreateRangedDownloadStream(r.ctx, partObjectName(r.folder, partIndex), partOffset, count)
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
		r.currentCount = count
		r.index++
		r.partOffset = 0
		r.mu.Unlock()
		return stream, nil
	}
}

func (r *rangedPartsReadCloser) finishPart(stream io.ReadCloser) error {
	r.mu.Lock()
	if r.current != stream {
		r.mu.Unlock()
		return nil
	}
	r.current = nil
	r.remaining -= r.currentCount
	r.currentCount = 0
	r.mu.Unlock()
	return stream.Close()
}
