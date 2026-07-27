//go:build !windows && !linux

package cache

func processRSSBytes() uint64 {
	return 0
}
