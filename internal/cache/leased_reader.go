package cache

import (
	"context"
	"errors"
	"io"
	"sync"
	"time"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/storagelifecycle"
)

type leasedReadCloser struct {
	stream    io.ReadCloser
	lifecycle *storagelifecycle.Service
	lease     *storagelifecycle.ReaderLease

	renewCancel context.CancelFunc
	renewDone   chan struct{}
	closeOnce   sync.Once
	closeErr    error
	errMu       sync.RWMutex
	leaseErr    error
}

func newLeasedReadCloser(stream io.ReadCloser, lifecycle *storagelifecycle.Service, lease *storagelifecycle.ReaderLease) *leasedReadCloser {
	renewCtx, renewCancel := context.WithCancel(context.Background())
	reader := &leasedReadCloser{
		stream:      stream,
		lifecycle:   lifecycle,
		lease:       lease,
		renewCancel: renewCancel,
		renewDone:   make(chan struct{}),
	}
	go reader.renew(renewCtx)
	return reader
}

func (r *leasedReadCloser) Read(p []byte) (int, error) {
	if err := r.currentLeaseError(); err != nil {
		return 0, err
	}
	n, err := r.stream.Read(p)
	if leaseErr := r.currentLeaseError(); leaseErr != nil {
		if n > 0 {
			return n, nil
		}
		return 0, errors.Join(err, leaseErr)
	}
	return n, err
}

func (r *leasedReadCloser) Close() error {
	r.closeOnce.Do(func() {
		r.renewCancel()
		<-r.renewDone
		streamErr := r.stream.Close()
		cleanupCtx, cancel := context.WithTimeout(context.Background(), mergeCleanupTimeout)
		releaseErr := r.lifecycle.ReleaseReader(cleanupCtx, r.lease.ID)
		cancel()
		r.closeErr = errors.Join(streamErr, releaseErr, r.currentLeaseError())
	})
	return r.closeErr
}

func (r *leasedReadCloser) renew(ctx context.Context) {
	defer close(r.renewDone)
	ticker := time.NewTicker(r.lease.RenewAfter)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			renewCtx, cancel := context.WithTimeout(ctx, mergeCleanupTimeout)
			err := r.lifecycle.RenewReader(renewCtx, r.lease.ID)
			cancel()
			if err != nil {
				if ctx.Err() != nil {
					return
				}
				r.errMu.Lock()
				r.leaseErr = err
				r.errMu.Unlock()
				_ = r.stream.Close()
				return
			}
		}
	}
}

func (r *leasedReadCloser) currentLeaseError() error {
	r.errMu.RLock()
	defer r.errMu.RUnlock()
	return r.leaseErr
}
