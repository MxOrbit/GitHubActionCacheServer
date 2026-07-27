//go:build linux

package cache

import (
	"os"
	"strconv"
	"strings"
)

func processRSSBytes() uint64 {
	contents, err := os.ReadFile("/proc/self/statm")
	if err != nil {
		return 0
	}
	fields := strings.Fields(string(contents))
	if len(fields) < 2 {
		return 0
	}
	residentPages, err := strconv.ParseUint(fields[1], 10, 64)
	if err != nil {
		return 0
	}
	return residentPages * uint64(os.Getpagesize())
}
