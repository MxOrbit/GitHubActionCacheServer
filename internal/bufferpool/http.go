package bufferpool

import (
	"io"
	"net/http"
)

func WrapTransport(base http.RoundTripper) http.RoundTripper {
	return Default.WrapTransport(base)
}

func (p *Pool) WrapTransport(base http.RoundTripper) http.RoundTripper {
	if base == nil {
		base = http.DefaultTransport
	}
	return bodyWriteToTransport{
		base: base,
		pool: p,
	}
}

type bodyWriteToTransport struct {
	base http.RoundTripper
	pool *Pool
}

func (t bodyWriteToTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body != nil && req.Body != http.NoBody {
		if _, ok := req.Body.(io.WriterTo); ok {
			return t.base.RoundTrip(req)
		}
		req = req.Clone(req.Context())
		req.Body = t.pool.WithWriteTo(req.Body)
	}
	return t.base.RoundTrip(req)
}
