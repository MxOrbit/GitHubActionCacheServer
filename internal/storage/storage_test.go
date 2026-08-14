package storage

import (
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestExactLengthReadCloserClampsUnderlyingRead(t *testing.T) {
	stream := newExactLengthReadCloser(strings.NewReader("abcdef"), io.NopCloser(strings.NewReader("")), 3)
	buffer := make([]byte, 8)

	n, err := stream.Read(buffer)
	require.NoError(t, err)
	require.Equal(t, 3, n)
	require.Equal(t, "abc", string(buffer[:n]))
	n, err = stream.Read(buffer)
	require.ErrorIs(t, err, io.EOF)
	require.Zero(t, n)
	require.NoError(t, stream.Close())
}
