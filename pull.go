package singleply

import (
	"fmt"
	"os"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
)

type FileStat struct {
	IsDir bool
	Size  uint64
	Name  string
}

type Connector interface {
	ListDir(path string, status StatusCallback) ([]*FileStat, error)
	PrepareForRead(path string, localPath string, offset uint64, length uint64, status StatusCallback) (prepared Region, err error)
}

type Region struct {
	Offset uint64
	Length uint64
}

type FS struct {
	connector Connector
	cache     Cache
	tracker   Tracker
}

func (f *FS) Root() (fs.Node, error) {
	return &Dir{path: "", fs: f}, nil
}

func (fs *FS) ListDir(path string) ([]*FileStat, error) {
	files, err := fs.ListDir(path)
	if err != nil {
		return nil, err
	}

	if files != nil {
		return files, nil
	}

	state := fs.tracker.AddOperation(fmt.Sprintf("ListDir(%s)", path))
	files, err = fs.connector.ListDir(path, state)
	fs.tracker.OperationComplete(state)

	if err != nil {
		return nil, err
	}

	var de DirEntries
	de = files
	fs.cache.PutListDir(path, &de)

	return files, nil
}

func (fs *FS) PrepareForRead(path string, localPath string, offset uint64, length uint64, status StatusCallback) error {
	for {
		region := fs.cache.GetFirstMissingRegion(path, offset, length)
		if region == nil {
			break
		}

		state := fs.tracker.AddOperation(fmt.Sprintf("PrepareForRead(%s, %d, %d)", path, region.Offset, region.Length))
		prepared, err := fs.connector.PrepareForRead(path, localPath, region.Offset, region.Length, state)
		fs.tracker.OperationComplete(state)
		if err != nil {
			return err
		}

		fs.cache.AddedRegions(path, prepared.Offset, prepared.Length)
	}

	return nil
}

type FileHandle struct {
	path string
	fs   *FS
	file *os.File
}

type Dir struct {
	path string
	fs   *FS
}

type File struct {
	path string
	fs   *FS
	size uint64
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0777
	fmt.Printf("Attr(%s) -> %o\n", d.path, a.Mode)
	return nil
}

func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	files, err := d.fs.ListDir(d.path)
	if err != nil {
		return nil, err
	}

	var ff DirEntries
	ff = files
	entry := ff.Get(name)
	if entry == nil {
		return nil, fuse.ENOENT
	}
	if entry.IsDir {
		return &Dir{path: d.path + "/" + name, fs: d.fs}, nil
	} else {
		return &File{path: d.path + "/" + name, fs: d.fs}, nil
	}
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	files, err := d.fs.ListDir(d.path)
	if err != nil {
		return nil, err
	}

	dirDirs := make([]fuse.Dirent, len(files))
	for i := 0; i < len(files); i++ {
		dirDirs[i].Name = files[i].Name
		if files[i].IsDir {
			dirDirs[i].Type = fuse.DT_Dir
		} else {
			dirDirs[i].Type = fuse.DT_File
		}
	}

	return dirDirs, nil
}

func (f *FileHandle) Forget() {
	f.file.Close()
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	//	a.Inode = 2
	a.Mode = 0444
	a.Size = f.size
	return nil
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {

	fmt.Printf("open(%s)\n", f.path)

	localPath := f.fs.cache.GetLocalFile(f.path, f.size)
	localFile, err := os.Open(localPath)
	if err != nil {
		return nil, err
	}

	return &FileHandle{path: f.path, fs: f.fs, file: localFile}, nil
}

func (f *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	err := f.fs.PrepareForRead(f.path, f.file.Name(), uint64(req.Offset), uint64(req.Size), nil)
	if err != nil {
		return err
	}

	buffer := make([]byte, req.Size)
	n, err := f.file.ReadAt(buffer, req.Offset)
	if err != nil {
		return err
	}

	// TODO: check, did caller allocate Data before this call?
	resp.Data = buffer[:n]
	return err
}
