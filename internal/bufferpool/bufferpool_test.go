package bufferpool

import (
	"bytes"
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestPoolGetPutUsesConfiguredSize(t *testing.T) {
	p := New(7)

	buf := p.Get()
	require.Len(t, buf, 7)

	buf = buf[:3]
	p.Put(buf)

	reused := p.Get()
	require.Len(t, reused, 7)
}

func TestPoolCopyCopiesWithPooledBuffer(t *testing.T) {
	p := New(5)
	reader := &observedReader{data: []byte("hello buffered world")}
	var dst recordingWriter

	written, err := p.Copy(&dst, reader)

	require.NoError(t, err)
	require.Equal(t, int64(len("hello buffered world")), written)
	require.Equal(t, "hello buffered world", dst.String())
	require.Equal(t, []int{5, 5, 5, 5, 5}, reader.readBufferSizes)
}

func TestPoolCopyKeepsReaderWriterFastPaths(t *testing.T) {
	src := &writerToReader{Reader: strings.NewReader("payload")}
	dst := &readerFromWriter{}

	written, err := New(3).Copy(dst, src)

	require.NoError(t, err)
	require.Equal(t, int64(len("payload")), written)
	require.Equal(t, "payload", dst.String())
	require.True(t, src.writeToCalled)
	require.False(t, dst.readFromCalled)
}

func TestPoolCopyReportsShortWrite(t *testing.T) {
	written, err := New(4).Copy(shortWriter{}, strings.NewReader("payload"))

	require.Equal(t, int64(1), written)
	require.ErrorIs(t, err, io.ErrShortWrite)
}

func TestPoolCopyReportsReadError(t *testing.T) {
	readErr := errors.New("read failed")
	written, err := New(4).Copy(io.Discard, errReader{err: readErr})

	require.Zero(t, written)
	require.ErrorIs(t, err, readErr)
}

func TestWithWriteToAddsWriterToUsingPooledBuffer(t *testing.T) {
	p := New(4)
	reader := &observedReadCloser{observedReader: observedReader{data: []byte("payload")}}
	wrapped := p.WithWriteTo(reader)
	writerTo, ok := wrapped.(io.WriterTo)
	require.True(t, ok)

	var dst recordingWriter
	written, err := writerTo.WriteTo(&dst)

	require.NoError(t, err)
	require.Equal(t, int64(len("payload")), written)
	require.Equal(t, "payload", dst.String())
	require.Equal(t, []int{4, 4, 4}, reader.readBufferSizes)
	require.NoError(t, wrapped.Close())
	require.True(t, reader.closed)
}

func TestWithWriteToKeepsExistingWriterTo(t *testing.T) {
	reader := &writerToReadCloser{writerToReader: writerToReader{Reader: strings.NewReader("payload")}}

	wrapped := New(4).WithWriteTo(reader)

	require.Same(t, reader, wrapped)
}

func TestWrapTransportAddsWriterToToRequestBody(t *testing.T) {
	p := New(4)
	body := &observedReadCloser{observedReader: observedReader{data: []byte("payload")}}
	req, err := http.NewRequest(http.MethodPost, "http://example.test", body)
	require.NoError(t, err)
	_, alreadyWriterTo := req.Body.(io.WriterTo)
	require.False(t, alreadyWriterTo)

	transport := p.WrapTransport(roundTripFunc(func(out *http.Request) (*http.Response, error) {
		require.NotSame(t, req, out)
		writerTo, ok := out.Body.(io.WriterTo)
		require.True(t, ok)

		var dst recordingWriter
		written, err := writerTo.WriteTo(&dst)
		require.NoError(t, err)
		require.Equal(t, int64(len("payload")), written)
		require.Equal(t, "payload", dst.String())
		require.Equal(t, []int{4, 4, 4}, body.readBufferSizes)

		return &http.Response{
			StatusCode: http.StatusNoContent,
			Body:       http.NoBody,
		}, nil
	}))

	res, err := transport.RoundTrip(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, res.StatusCode)
	_, stillNoWriterTo := req.Body.(io.WriterTo)
	require.False(t, stillNoWriterTo)
}

func TestWrapTransportKeepsExistingWriterToRequestBody(t *testing.T) {
	body := &writerToReadCloser{writerToReader: writerToReader{Reader: strings.NewReader("payload")}}
	req, err := http.NewRequest(http.MethodPost, "http://example.test", body)
	require.NoError(t, err)

	transport := New(4).WrapTransport(roundTripFunc(func(out *http.Request) (*http.Response, error) {
		require.Same(t, req, out)
		require.Same(t, body, out.Body)
		return &http.Response{
			StatusCode: http.StatusNoContent,
			Body:       http.NoBody,
		}, nil
	}))

	res, err := transport.RoundTrip(req)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, res.StatusCode)
}

func TestNewPanicsForInvalidSize(t *testing.T) {
	require.Panics(t, func() {
		New(0)
	})
}

type observedReader struct {
	data            []byte
	readBufferSizes []int
}

func (r *observedReader) Read(p []byte) (int, error) {
	r.readBufferSizes = append(r.readBufferSizes, len(p))
	if len(r.data) == 0 {
		return 0, io.EOF
	}
	n := copy(p, r.data)
	r.data = r.data[n:]
	return n, nil
}

type observedReadCloser struct {
	observedReader
	closed bool
}

func (r *observedReadCloser) Close() error {
	r.closed = true
	return nil
}

type recordingWriter struct {
	data []byte
}

func (w *recordingWriter) Write(p []byte) (int, error) {
	w.data = append(w.data, p...)
	return len(p), nil
}

func (w *recordingWriter) String() string {
	return string(w.data)
}

type writerToReader struct {
	*strings.Reader
	writeToCalled bool
}

func (r *writerToReader) WriteTo(w io.Writer) (int64, error) {
	r.writeToCalled = true
	return r.Reader.WriteTo(w)
}

type writerToReadCloser struct {
	writerToReader
}

func (r *writerToReadCloser) Close() error {
	return nil
}

type readerFromWriter struct {
	bytes.Buffer
	readFromCalled bool
}

func (w *readerFromWriter) ReadFrom(r io.Reader) (int64, error) {
	w.readFromCalled = true
	return w.Buffer.ReadFrom(r)
}

type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	return 1, nil
}

type errReader struct {
	err error
}

func (r errReader) Read([]byte) (int, error) {
	return 0, r.err
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) {
	return f(req)
}
