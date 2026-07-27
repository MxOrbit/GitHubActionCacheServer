//go:build windows

package cache

import (
	"syscall"
	"unsafe"
)

var getProcessMemoryInfo = syscall.NewLazyDLL("psapi.dll").NewProc("GetProcessMemoryInfo")

type processMemoryCounters struct {
	Size                       uint32
	PageFaultCount             uint32
	PeakWorkingSetSize         uintptr
	WorkingSetSize             uintptr
	QuotaPeakPagedPoolUsage    uintptr
	QuotaPagedPoolUsage        uintptr
	QuotaPeakNonPagedPoolUsage uintptr
	QuotaNonPagedPoolUsage     uintptr
	PagefileUsage              uintptr
	PeakPagefileUsage          uintptr
}

func processRSSBytes() uint64 {
	handle, err := syscall.GetCurrentProcess()
	if err != nil {
		return 0
	}
	counters := processMemoryCounters{Size: uint32(unsafe.Sizeof(processMemoryCounters{}))}
	result, _, _ := getProcessMemoryInfo.Call(
		uintptr(handle),
		uintptr(unsafe.Pointer(&counters)),
		uintptr(counters.Size),
	)
	if result == 0 {
		return 0
	}
	return uint64(counters.WorkingSetSize)
}
