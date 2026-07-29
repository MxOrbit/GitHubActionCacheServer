//go:build windows

package storage

import (
	"fmt"
	"math"

	"golang.org/x/sys/windows"
)

func filesystemUsage(path string) (FilesystemUsage, error) {
	pathPointer, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return FilesystemUsage{}, fmt.Errorf("encode filesystem path: %w", err)
	}
	var availableBytes uint64
	var capacityBytes uint64
	var freeBytes uint64
	if err := windows.GetDiskFreeSpaceEx(pathPointer, &availableBytes, &capacityBytes, &freeBytes); err != nil {
		return FilesystemUsage{}, fmt.Errorf("inspect filesystem usage: %w", err)
	}
	if capacityBytes > math.MaxInt64 || availableBytes > capacityBytes {
		return FilesystemUsage{}, fmt.Errorf("filesystem usage exceeds supported range")
	}
	return FilesystemUsage{
		CapacityBytes: int64(capacityBytes),
		UsedBytes:     int64(capacityBytes - availableBytes),
	}, nil
}
