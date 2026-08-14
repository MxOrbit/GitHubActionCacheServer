package handler

import (
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/MxOrbit/GitHubActionCacheServer/internal/cache"
)

const (
	maxRangeHeaderBytes = 256
	maxRangeSpecifiers  = 8
)

var errMalformedRange = errors.New("malformed byte range")

func parseDownloadRanges(header http.Header) ([]cache.ByteRange, bool, error) {
	value, present, xmsRange, err := preferredRangeHeader(header)
	if err != nil || !present {
		return nil, false, err
	}
	if len(header.Values("If-Range")) != 0 {
		// This endpoint does not issue validators that make If-Range meaningful.
		// Preserve RFC fallback for Range, but fail closed for Azure's x-ms-range:
		// azblob accepts a full 200 response for a resumed transfer without
		// checking Content-Range, which would silently misalign the payload.
		if xmsRange {
			return nil, false, errMalformedRange
		}
		return nil, false, nil
	}
	if len(value) > maxRangeHeaderBytes {
		return nil, false, errMalformedRange
	}
	unit, set, ok := strings.Cut(value, "=")
	if !ok {
		return nil, false, errMalformedRange
	}
	if !strings.EqualFold(strings.TrimSpace(unit), "bytes") {
		return nil, false, nil
	}

	parts := strings.Split(set, ",")
	if len(parts) == 0 || len(parts) > maxRangeSpecifiers {
		return nil, false, errMalformedRange
	}
	ranges := make([]cache.ByteRange, 0, len(parts))
	for _, raw := range parts {
		spec := strings.TrimSpace(raw)
		if spec == "" {
			return nil, false, errMalformedRange
		}
		if strings.HasPrefix(spec, "-") {
			if strings.Contains(spec[1:], "-") {
				return nil, false, errMalformedRange
			}
			length, err := parseRangeDecimal(spec[1:])
			if err != nil {
				return nil, false, errMalformedRange
			}
			ranges = append(ranges, cache.SuffixByteRange(length))
			continue
		}

		firstText, lastText, ok := strings.Cut(spec, "-")
		if !ok || strings.Contains(lastText, "-") {
			return nil, false, errMalformedRange
		}
		first, err := parseRangeDecimal(firstText)
		if err != nil {
			return nil, false, errMalformedRange
		}
		if lastText == "" {
			ranges = append(ranges, cache.OpenEndedByteRange(first))
			continue
		}
		last, err := parseRangeDecimal(lastText)
		if err != nil || last < first {
			return nil, false, errMalformedRange
		}
		ranges = append(ranges, cache.ClosedByteRange(first, last))
	}
	return ranges, true, nil
}

func preferredRangeHeader(header http.Header) (string, bool, bool, error) {
	xmsValues := header.Values("X-Ms-Range")
	if len(xmsValues) > 1 {
		return "", false, false, errMalformedRange
	}
	if len(xmsValues) == 1 {
		if value := strings.TrimSpace(xmsValues[0]); value != "" {
			return value, true, true, nil
		}
	}
	rangeValues := header.Values("Range")
	if len(rangeValues) > 1 {
		return "", false, false, errMalformedRange
	}
	if len(rangeValues) == 1 {
		if value := strings.TrimSpace(rangeValues[0]); value != "" {
			return value, true, false, nil
		}
	}
	return "", false, false, nil
}

func parseRangeDecimal(value string) (int64, error) {
	if value == "" {
		return 0, errMalformedRange
	}
	for _, char := range value {
		if char < '0' || char > '9' {
			return 0, errMalformedRange
		}
	}
	return strconv.ParseInt(value, 10, 64)
}
