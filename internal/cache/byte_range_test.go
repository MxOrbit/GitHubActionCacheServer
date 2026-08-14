package cache

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestResolveByteRangesSelectsFirstSatisfiableRange(t *testing.T) {
	tests := []struct {
		name   string
		ranges []ByteRange
		size   int64
		want   DownloadRange
	}{
		{name: "closed clamps end", ranges: []ByteRange{ClosedByteRange(2, 20)}, size: 9, want: DownloadRange{Offset: 2, Count: 7}},
		{name: "open ended", ranges: []ByteRange{OpenEndedByteRange(2)}, size: 9, want: DownloadRange{Offset: 2, Count: 7}},
		{name: "suffix", ranges: []ByteRange{SuffixByteRange(3)}, size: 9, want: DownloadRange{Offset: 6, Count: 3}},
		{name: "large suffix", ranges: []ByteRange{SuffixByteRange(20)}, size: 9, want: DownloadRange{Offset: 0, Count: 9}},
		{name: "later satisfiable", ranges: []ByteRange{ClosedByteRange(99, 100), ClosedByteRange(1, 2)}, size: 9, want: DownloadRange{Offset: 1, Count: 2}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := resolveByteRanges(test.ranges, test.size)
			require.NoError(t, err)
			require.Equal(t, test.want, got)
		})
	}
}

func TestResolveByteRangesReportsUnsatisfiableSize(t *testing.T) {
	for _, test := range []struct {
		name string
		spec ByteRange
		size int64
	}{
		{name: "start at end", spec: OpenEndedByteRange(4), size: 4},
		{name: "zero suffix", spec: SuffixByteRange(0), size: 4},
		{name: "empty object", spec: SuffixByteRange(1), size: 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, err := resolveByteRanges([]ByteRange{test.spec}, test.size)
			var rangeErr *RangeNotSatisfiableError
			require.ErrorAs(t, err, &rangeErr)
			require.Equal(t, test.size, rangeErr.SizeBytes)
		})
	}
}
