package singleply

import (
	"sync"
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

	lock                 sync.Mutex
	ReadRequestLengths   []uint64
	ConnectorReadLengths []uint64
}

func (s *Stats) RecordConnectorReadLen(length uint64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if len(s.ConnectorReadLengths) < 1000 {
		s.ConnectorReadLengths = append(s.ConnectorReadLengths, length)
	}
}

func (s *Stats) RecordReadRequestLen(length uint64) {
	s.lock.Lock()
	defer s.lock.Unlock()
	if len(s.ReadRequestLengths) < 1000 {
		s.ReadRequestLengths = append(s.ReadRequestLengths, length)
	}
}

func (s *Stats) IncInvalidatedDirCount() {
	atomic.AddInt32(&s.InvalidatedDirCount, 1)
}

func (s *Stats) IncGotStaleDirCount() {
	atomic.AddInt32(&s.GotStaleDirCount, 1)
}

func (s *Stats) IncListDirSuccessCount() {
	atomic.AddInt32(&s.ListDirSuccessCount, 1)
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
