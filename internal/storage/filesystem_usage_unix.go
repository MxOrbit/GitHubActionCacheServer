//go:build aix || android || darwin || dragonfly || freebsd || illumos || ios || linux || netbsd || openbsd || solaris

package storage

import (
	"fmt"
	"math"

	"golang.org/x/sys/unix"
)

func filesystemUsage(path string) (FilesystemUsage, error) {
	var stats unix.Statfs_t
	if err := unix.Statfs(path, &stats); err != nil {
		return FilesystemUsage{}, fmt.Errorf("inspect filesystem usage: %w", err)
	}
	capacityBytes, ok := multiplyFilesystemBlocks(uint64(stats.Blocks), uint64(stats.Bsize))
	if !ok {
		return FilesystemUsage{}, fmt.Errorf("filesystem capacity exceeds supported range")
	}
	usedBlocks := uint64(stats.Blocks) - min(uint64(stats.Blocks), uint64(stats.Bavail))
	usedBytes, ok := multiplyFilesystemBlocks(usedBlocks, uint64(stats.Bsize))
	if !ok {
		return FilesystemUsage{}, fmt.Errorf("filesystem usage exceeds supported range")
	}
	return FilesystemUsage{CapacityBytes: capacityBytes, UsedBytes: usedBytes}, nil
}

func multiplyFilesystemBlocks(blocks, blockSize uint64) (int64, bool) {
	if blockSize != 0 && blocks > math.MaxInt64/blockSize {
		return 0, false
	}
	return int64(blocks * blockSize), true
}
