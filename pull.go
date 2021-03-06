package singleply

import (
	"fmt"
	"os"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
	"errors"
)

type FileStat struct {
	IsDir bool
	Size  uint64
	Name  string
	Etag  string
}

type Connector interface {
	ListDir(path string, status StatusCallback) (*DirEntries, error)
	PrepareForRead(path string, etag string, localPath string, offset uint64, length uint64, status StatusCallback) (prepared *Region, err error)
}

type Region struct {
	Offset uint64
	Length uint64
}

type FS struct {
	connector Connector
	cache     Cache
	tracker   *Tracker
	stats *Stats
}

func NewFileSystem(connector Connector, cache Cache, tracker *Tracker, stats *Stats) *FS {
	return &FS{connector: connector, cache: cache, tracker: tracker, stats: stats}
}

func (f *FS) Root() (fs.Node, error) {
	return &Dir{path: "", fs: f}, nil
}

func (fs *FS) cleanupOldSnapshot(path string, oldFiles *DirEntries, newFiles *DirEntries) error {
	current := make(map[string]string)
	for _, file := range newFiles.Files {
		current[file.Name] = file.Etag
	}
	
	for _, file := range oldFiles.Files {
		// for each file, if it no longer exists or has changed, evict it from the cache
		currentEtag, present := current[file.Name]
		if !present || currentEtag != file.Etag {
 			err := fs.cache.EvictFile(path + "/" + file.Name)
			
			if err == nil {
				fs.stats.IncFilesEvicted()
			} else if err != NotInCache {
				return err
			} 		
		}
	}
	
	return nil
}

func (fs *FS) ListDir(path string) (*DirEntries, error) {
	cachedDir, err := fs.cache.GetListDir(path)
	if err != nil {
		fmt.Printf("cache.GetListDir returned error: %s\n", err.Error())
		return nil, err
	}

	if cachedDir != nil {
		fmt.Printf("found dir \"%s\" in cache\n", path)
		if cachedDir.Valid {
			return cachedDir, nil
		} else {
			fs.stats.IncGotStaleDirCount()
		}
	}

	fmt.Printf("did not find dir \"%s\" in cache\n", path)
	
	state := fs.tracker.AddOperation(fmt.Sprintf("ListDir(%s)", path))
	files, err := fs.connector.ListDir(path, state)
	fs.tracker.OperationComplete(state)

	if err != nil {
		fmt.Printf("ListDir returned error: %s\n", err.Error())
		fs.stats.IncListDirFailedCount()
		return nil, err
	}
	
	fmt.Printf("calling IncListDirSuccessCount ------\n")
	fs.stats.IncListDirSuccessCount()

	fmt.Printf("storing dir \"%s\" in cache\n", path)
	files.Valid = true
	
	if cachedDir != nil {
		// if we reached here, we had a previous snapshot for this dir, and we actually need to clean up the unused entries in the old snapshot
		err = fs.cleanupOldSnapshot(path, cachedDir, files)
		if err != nil {
			return nil, err
		}
	}
	
	err = fs.cache.PutListDir(path, files)
	if err != nil {
		return nil, err
	}

	return files, nil
}

func (fs *FS) PrepareForRead(path string, etag, localPath string, offset uint64, length uint64, status StatusCallback) error {
	for {
		region := fs.cache.GetFirstMissingRegion(path, offset, length)
		if region == nil {
			break
		}

		fmt.Printf("Fetching region %s to fulfill read of (offset: %d, len: %s) for %s\n", region, offset, length, path)
		state := fs.tracker.AddOperation(fmt.Sprintf("PrepareForRead(%s, %d, %d)", path, region.Offset, region.Length))
		prepared, err := fs.connector.PrepareForRead(path, etag, localPath, region.Offset, region.Length, state)
		fs.tracker.OperationComplete(state)
		if err != nil {
			fs.stats.IncPrepareForReadFailedCount()
			return err
		}

		fs.stats.IncPrepareForReadSuccessCount()
		fs.stats.IncBytesRead(int64(prepared.Length))
		
		if prepared.Offset > region.Offset || (prepared.Offset + prepared.Length) < (region.Offset + region.Length) {
			return errors.New(fmt.Sprintf("Requested region %s but got %s", region, prepared))
		}
		
		fs.cache.AddedRegions(path, prepared.Offset, prepared.Length)
	}

	return nil
}

type FileHandle struct {
	path string
	fs   *FS
	file *os.File
	etag string
}

type Dir struct {
	path string
	fs   *FS
}

type File struct {
	path string
	fs   *FS
	size uint64
	etag string
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0777
	fmt.Printf("Dir.Attr(%s) -> %o\n", d.path, a.Mode)
	return nil
}

func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	files, err := d.fs.ListDir(d.path)
	if err != nil {
		return nil, err
	}

	entry := files.Get(name)
	if entry == nil {
		//if len(files.Files) == 0 {
		fmt.Printf("Could not find entry for \"%s\" among %d entries for %s\n", name, len(files.Files), d.path)
		//}
		return nil, fuse.ENOENT
	}

	var childName string
	if d.path == "" {
		childName = name
	} else {
		childName = d.path + "/" + name
	}
	if entry.IsDir {
		return &Dir{path: childName, fs: d.fs}, nil
	} else {
		return &File{path: childName, fs: d.fs, size: entry.Size, etag: entry.Etag}, nil
	}
}

func (d *Dir) ReadDirAll(ctx context.Context) ([]fuse.Dirent, error) {
	filesv, err := d.fs.ListDir(d.path)
	if err != nil {
		return nil, err
	}
	files := filesv.Files

	fmt.Printf("Dir \"%s\" had %d entries:", d.path, len(files))
	for _, f := range files {
		fmt.Printf("\"%s\" ", f.Name)
	}
	fmt.Printf("\n")

	dirDirs := make([]fuse.Dirent, len(files))
	for i := 0; i < len(files); i++ {
		dirDirs[i].Name = files[i].Name
		if files[i].IsDir {
			dirDirs[i].Type = fuse.DT_Dir
		} else {
			dirDirs[i].Type = fuse.DT_File
		}
	}

	fmt.Printf("returning Dirent with %d entries\n", len(dirDirs))
	return dirDirs, nil
}

func (f *FileHandle) Forget() {
	f.file.Close()
}

func (f *File) Attr(ctx context.Context, a *fuse.Attr) error {
	//	a.Inode = 2
	fmt.Printf("File.Attr(%s) -> size=%d\n", f.path, f.size)
	a.Mode = 0444
	a.Size = f.size
	return nil
}

func (f *File) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {

	fmt.Printf("open(%s)\n", f.path)

	localPath, err := f.fs.cache.GetLocalFile(f.path, f.size)
	if err != nil {
		return nil, err
	}
	localFile, err := os.Open(localPath)
	if err != nil {
		return nil, err
	}

	return &FileHandle{path: f.path, fs: f.fs, file: localFile, etag: f.etag}, nil
}

func (f *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	err := f.fs.PrepareForRead(f.path, f.etag, f.file.Name(), uint64(req.Offset), uint64(req.Size), nil)
	if err != nil {
		fmt.Printf("PrepareForRead failed: %s\n", err.Error())
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
