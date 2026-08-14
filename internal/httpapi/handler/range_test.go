package handler

import (
	"net/http"
	"strings"
	"testing"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/cache"
	"github.com/stretchr/testify/require"
)

func TestParseDownloadRanges(t *testing.T) {
	tests := []struct {
		name    string
		header  http.Header
		want    []cache.ByteRange
		present bool
		wantErr bool
	}{
		{name: "absent", header: http.Header{}},
		{name: "closed", header: http.Header{"Range": {"bytes=2-5"}}, want: []cache.ByteRange{cache.ClosedByteRange(2, 5)}, present: true},
		{name: "open ended", header: http.Header{"Range": {"bytes=2-"}}, want: []cache.ByteRange{cache.OpenEndedByteRange(2)}, present: true},
		{name: "suffix", header: http.Header{"Range": {"bytes=-5"}}, want: []cache.ByteRange{cache.SuffixByteRange(5)}, present: true},
		{name: "multiple", header: http.Header{"Range": {"bytes=99-100, 2-3"}}, want: []cache.ByteRange{cache.ClosedByteRange(99, 100), cache.ClosedByteRange(2, 3)}, present: true},
		{name: "x ms wins", header: http.Header{"X-Ms-Range": {"bytes=2-3"}, "Range": {"bytes=0-1"}}, want: []cache.ByteRange{cache.ClosedByteRange(2, 3)}, present: true},
		{name: "empty x ms falls through", header: http.Header{"X-Ms-Range": {""}, "Range": {"bytes=1-"}}, want: []cache.ByteRange{cache.OpenEndedByteRange(1)}, present: true},
		{name: "unknown unit ignored", header: http.Header{"Range": {"items=0-1"}}},
		{name: "if range ignored", header: http.Header{"Range": {"bytes=1-"}, "If-Range": {"etag"}}},
		{name: "if range with x ms fails closed", header: http.Header{"X-Ms-Range": {"bytes=1-"}, "If-Range": {"etag"}}, wantErr: true},
		{name: "case insensitive unit", header: http.Header{"Range": {"BYTES=1-2"}}, want: []cache.ByteRange{cache.ClosedByteRange(1, 2)}, present: true},
		{name: "empty set", header: http.Header{"Range": {"bytes="}}, wantErr: true},
		{name: "descending", header: http.Header{"Range": {"bytes=5-3"}}, wantErr: true},
		{name: "embedded spaces", header: http.Header{"Range": {"bytes=0 - 5"}}, wantErr: true},
		{name: "overflow", header: http.Header{"Range": {"bytes=0-9223372036854775808"}}, wantErr: true},
		{name: "too long", header: http.Header{"Range": {"bytes=" + strings.Repeat("1", maxRangeHeaderBytes)}}, wantErr: true},
		{name: "too many", header: http.Header{"Range": {"bytes=0-0,1-1,2-2,3-3,4-4,5-5,6-6,7-7,8-8"}}, wantErr: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, present, err := parseDownloadRanges(test.header)
			if test.wantErr {
				require.ErrorIs(t, err, errMalformedRange)
				return
			}
			require.NoError(t, err)
			require.Equal(t, test.present, present)
			require.Equal(t, test.want, got)
		})
	}
}

func TestParseDownloadRangesRejectsRepeatedFieldLines(t *testing.T) {
	for _, name := range []string{"Range", "X-Ms-Range"} {
		t.Run(name, func(t *testing.T) {
			header := http.Header{}
			header.Add(name, "bytes=0-1")
			header.Add(name, "bytes=2-3")
			_, _, err := parseDownloadRanges(header)
			require.ErrorIs(t, err, errMalformedRange)
		})
	}
}
