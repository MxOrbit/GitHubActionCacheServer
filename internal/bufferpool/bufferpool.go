package bufferpool

import (
	"io"
	"sync"
)

const DefaultSize = 32 * 1024

var Default = New(DefaultSize)

type Pool struct {
	size int
	pool sync.Pool
}

func New(size int) *Pool {
	if size < 1 {
		panic("bufferpool: size must be positive")
	}

	p := &Pool{size: size}
	p.pool.New = func() any {
		return make([]byte, size)
	}
	return p
}

func (p *Pool) Size() int {
	return p.size
}

func (p *Pool) Get() []byte {
	return p.pool.Get().([]byte)
}

func (p *Pool) Put(buf []byte) {
	if cap(buf) != p.size {
		return
	}
	p.pool.Put(buf[:p.size])
}

func Copy(dst io.Writer, src io.Reader) (int64, error) {
	return Default.Copy(dst, src)
}

func WithWriteTo(src io.ReadCloser) io.ReadCloser {
	return Default.WithWriteTo(src)
}

func (p *Pool) Copy(dst io.Writer, src io.Reader) (int64, error) {
	buf := p.Get()
	defer p.Put(buf)

	return io.CopyBuffer(dst, src, buf)
}

func (p *Pool) WithWriteTo(src io.ReadCloser) io.ReadCloser {
	if src == nil {
		return nil
	}
	if _, ok := src.(io.WriterTo); ok {
		return src
	}
	return writeToReadCloser{
		ReadCloser: src,
		pool:       p,
	}
}

type writeToReadCloser struct {
	io.ReadCloser
	pool *Pool
}

func (r writeToReadCloser) WriteTo(w io.Writer) (int64, error) {
	return r.pool.Copy(w, r.ReadCloser)
}
