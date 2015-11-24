package singleply

import (
	"bytes"
	"io"
	"os"
	"time"
	"golang.org/x/net/context"
	)

// just used for testing.  A connector which claims every dir has the same contents.  And every file contains just its own filename
type MockConn struct {
}

func (c *MockConn) PrepareForRead(ctx context.Context, path string, etag string, localPath string, offset uint64, length uint64, status StatusCallback) (prepared *Region, err error) {
	f, err := os.OpenFile(localPath, os.O_RDWR, 0)

	if err != nil {
		return nil, err
	}

	defer f.Close()

	content := []byte(path + "\n")
	src := bytes.NewBuffer(content)
	_, err = io.Copy(f, src)

	return &Region{0, uint64(len(content))}, nil
}

func (c *MockConn) ListDir(ctx context.Context,path string, status StatusCallback) (*DirEntries, error) {
	files := make([]*FileStat, 0, 100)
	files = append(files, &FileStat{Name: "dir1", IsDir: true, Size: uint64(0)})
	files = append(files, &FileStat{Name: "dir2", IsDir: true, Size: uint64(0)})
	files = append(files, &FileStat{Name: "file1", IsDir: false, Size: uint64(len(path) + 6)})
	files = append(files, &FileStat{Name: "file2", IsDir: false, Size: uint64(len(path) + 6)})
	return &DirEntries{Valid: true, Files: files}, nil
}

type DelayConn struct {
	delay time.Duration
	underlying Connector
}

func DelayConnector(delay time.Duration, conn Connector) Connector {
	return &DelayConn{delay: delay, underlying: conn}
} 

func (c *DelayConn) PrepareForRead(ctx context.Context,path string, etag string, localPath string, offset uint64, length uint64, status StatusCallback) (prepared *Region, err error) {
	time.Sleep(c.delay)
	return c.underlying.PrepareForRead(ctx, path, etag, localPath, offset, length, status)
}

func (c *DelayConn) ListDir(ctx context.Context,path string, status StatusCallback) (*DirEntries, error) {
	time.Sleep(c.delay)
	return c.ListDir(ctx, path, status)
}