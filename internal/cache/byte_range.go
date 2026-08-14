package cache

import (
	"errors"
	"fmt"
)

var ErrRangeNotSatisfiable = errors.New("requested byte range is not satisfiable")

type byteRangeKind uint8

const (
	closedByteRange byteRangeKind = iota
	openEndedByteRange
	suffixByteRange
)

// ByteRange is a parsed, representation-independent byte range. Constructors
// keep HTTP syntax out of the cache package while preserving the distinction
// between closed, open-ended, and suffix ranges until the total size is known.
type ByteRange struct {
	kind   byteRangeKind
	first  int64
	last   int64
	suffix int64
}

func ClosedByteRange(first, last int64) ByteRange {
	return ByteRange{kind: closedByteRange, first: first, last: last}
}

func OpenEndedByteRange(first int64) ByteRange {
	return ByteRange{kind: openEndedByteRange, first: first}
}

func SuffixByteRange(length int64) ByteRange {
	return ByteRange{kind: suffixByteRange, suffix: length}
}

type DownloadRange struct {
	Offset int64
	Count  int64
}

type RangeNotSatisfiableError struct {
	SizeBytes int64
}

func (e *RangeNotSatisfiableError) Error() string {
	return fmt.Sprintf("%s: size %d", ErrRangeNotSatisfiable, e.SizeBytes)
}

func (e *RangeNotSatisfiableError) Unwrap() error {
	return ErrRangeNotSatisfiable
}

func resolveByteRanges(ranges []ByteRange, size int64) (DownloadRange, error) {
	if size < 0 {
		return DownloadRange{}, fmt.Errorf("invalid cache size %d", size)
	}
	for _, candidate := range ranges {
		switch candidate.kind {
		case closedByteRange:
			if candidate.first < 0 || candidate.last < candidate.first {
				return DownloadRange{}, fmt.Errorf("invalid closed byte range %d-%d", candidate.first, candidate.last)
			}
			if candidate.first >= size {
				continue
			}
			last := min(candidate.last, size-1)
			return DownloadRange{Offset: candidate.first, Count: last - candidate.first + 1}, nil
		case openEndedByteRange:
			if candidate.first < 0 {
				return DownloadRange{}, fmt.Errorf("invalid open-ended byte range %d-", candidate.first)
			}
			if candidate.first >= size {
				continue
			}
			return DownloadRange{Offset: candidate.first, Count: size - candidate.first}, nil
		case suffixByteRange:
			if candidate.suffix < 0 {
				return DownloadRange{}, fmt.Errorf("invalid suffix byte range -%d", candidate.suffix)
			}
			if candidate.suffix == 0 || size == 0 {
				continue
			}
			count := min(candidate.suffix, size)
			return DownloadRange{Offset: size - count, Count: count}, nil
		default:
			return DownloadRange{}, fmt.Errorf("invalid byte range kind %d", candidate.kind)
		}
	}
	return DownloadRange{}, &RangeNotSatisfiableError{SizeBytes: size}
}
