package singleply

import (
	"fmt"
	"sync/atomic"
)

type Stats struct {
	ListDirSuccessCount        int32
	ListDirFailedCount         int32
	PrepareForReadSuccessCount int32
	PrepareForReadFailedCount  int32
	BytesRead                  int64
	FilesRead                  int32
	FilesEvicted               int32
	GotStaleDirCount           int32
	InvalidatedDirCount        int32
}

func (s *Stats) IncInvalidatedDirCount() {
	atomic.AddInt32(&s.InvalidatedDirCount, 1)
}

func (s *Stats) IncGotStaleDirCount() {
	atomic.AddInt32(&s.GotStaleDirCount, 1)
}

func (s *Stats) IncListDirSuccessCount() {
	v := atomic.AddInt32(&s.ListDirSuccessCount, 1)
	fmt.Printf("IncListDirSuccessCount() -> %d\n", v)
}

func (s *Stats) IncFilesEvicted() {
	atomic.AddInt32(&s.FilesEvicted, 1)
}

func (s *Stats) IncListDirFailedCount() {
	atomic.AddInt32(&s.ListDirFailedCount, 1)
}

func (s *Stats) IncPrepareForReadSuccessCount() {
	atomic.AddInt32(&s.PrepareForReadSuccessCount, 1)
}

func (s *Stats) IncPrepareForReadFailedCount() {
	atomic.AddInt32(&s.PrepareForReadFailedCount, 1)
}

func (s *Stats) IncBytesRead(count int64) {
	atomic.AddInt64(&s.BytesRead, count)
}

func (s *Stats) IncFilesRead() {
	atomic.AddInt32(&s.FilesRead, 1)
}
