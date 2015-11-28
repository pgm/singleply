package singleply

import (
	"fmt"
	"log"
	"os"
	"io"
	
	"errors"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
)

type FileStat struct {
	IsDir bool
	Size  uint64
	Name  string
	Etag  string
}

type Connector interface {
	ListDir(context context.Context, path string, status StatusCallback) (*DirEntries, error)
	PrepareForRead(context context.Context, path string, etag string, writer io.WriteSeeker, offset uint64, length uint64, status StatusCallback) (prepared *Region, err error)
}

type Region struct {
	Offset uint64
	Length uint64
}

type FS struct {
	connector Connector
	cache     Cache
	tracker   *Tracker
	stats     *Stats
	requestQueue chan *Req
	blockSize uint64
}

func NewFileSystem(connector Connector, cache Cache, tracker *Tracker, stats *Stats, workers int, blockSize uint64) *FS {
	queue := make(chan *Req)
	for i := 0 ;i<workers;i++ {
		go WorkerLoop(connector, queue)

	}
	return &FS{connector: connector, cache: cache, tracker: tracker, stats: stats, requestQueue: queue, blockSize: blockSize}
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

func (fs *FS) ListDir(ctx context.Context, path string) (*DirEntries, error) {
	cachedDir, err := fs.cache.GetListDir(path)
	if err != nil {
		fmt.Printf("cache.GetListDir returned error: %s\n", err.Error())
		return nil, err
	}

	if cachedDir != nil {
		//fmt.Printf("found dir \"%s\" in cache\n", path)
		if cachedDir.Valid {
			return cachedDir, nil
		} else {
			fs.stats.IncGotStaleDirCount()
		}
	}

	//fmt.Printf("did not find dir \"%s\" in cache\n", path)

	state := fs.tracker.AddOperation(fmt.Sprintf("ListDir(%s)", path))
	files, err := fs.connector.ListDir(ctx, path, state)
	fs.tracker.OperationComplete(state)

	if err != nil {
		fmt.Printf("ListDir returned error: %s\n", err.Error())
		fs.stats.IncListDirFailedCount()
		return nil, err
	}

	fs.stats.IncListDirSuccessCount()

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

type Resp struct {
	prepared *Region
	req *Req
	err error
}

type Req struct {
	ctx context.Context
	path string
	etag string
	writer io.WriteSeeker
	offset uint64
	length uint64
	status *State

	// upon completion either an error will be sent back or channel closed
	response chan *Resp
}

func WorkerLoop(connector Connector, queue chan *Req) {
	for {
		req, ok := <- queue
		if ! ok {
			break
		}
		
		prepared, err := connector.PrepareForRead(req.ctx, req.path, req.etag, req.writer, req.offset, req.length, req.status)
		req.response <- &Resp{prepared: prepared, req: req, err: err}
	}
}

func getMissingRegions(cache Cache,  path string, offset uint64, length uint64, blockSize uint64) [] *Region {
	regionEnd := offset+length
	missing := make([]*Region, 0, 100)
	for {
		next := cache.GetFirstMissingRegion(path, offset, regionEnd-offset)
		if next == nil {
			break
		}

		// divide missing into chunk sized pieces
		for start := (next.Offset / blockSize) * blockSize ; start < next.Offset + next.Length ; start += blockSize {
			missing = append(missing, &Region{start, blockSize})
			offset = start + blockSize
		}
	}
	
	return missing
}

func (fs *FS) PrepareForRead(ctx context.Context, path string, etag string, writer io.WriteSeeker, offset uint64, length uint64, status StatusCallback) error {
	regions := getMissingRegions(fs.cache, path, offset, length, fs.blockSize)
	if len(regions) == 0 {
		return nil
	}

	responses := make(chan *Resp)
	for _, region := range regions {
		fmt.Printf("Fetching region %s to fulfill read of (offset: %d, len: %s) for %s\n", region, offset, length, path)
		state := fs.tracker.AddOperation(fmt.Sprintf("PrepareForRead(%s, %d, %d)", path, region.Offset, region.Length))
		fs.requestQueue <- &Req{ctx: ctx, path: path, etag: etag, writer: writer, offset: offset, length: length, status: state, response: responses}
	}
	
	// block until all workers have responded
	var finalErr error = nil
	addedRegions := make([]*Region, 0, 100)
	for i:=0;i<len(regions);i++ {
		resp := <- responses
		
		fs.tracker.OperationComplete(resp.req.status)
		if resp.err != nil {
			fs.stats.IncPrepareForReadFailedCount()
			if finalErr == nil || resp.err != CanceledOperation {
				finalErr = resp.err
			}
		} else {
			fs.stats.IncPrepareForReadSuccessCount()
			prepared := resp.prepared
			req := resp.req
			fs.stats.IncBytesRead(int64(prepared.Length))

			if prepared.Offset > req.offset || (prepared.Offset+prepared.Length) < (req.offset+req.length) {
				return errors.New(fmt.Sprintf("Requested region (%s+%s) but got %s", req.offset, req.length, prepared))
			}
			addedRegions = append(addedRegions, prepared)
		}
	}
	
	for _, prepared := range addedRegions {
		//err := 
		fs.cache.AddedRegions(path, prepared.Offset, prepared.Length)
		//if err != nil {
		//	return err
		//}
	}
	
	return finalErr
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
	files, err := d.fs.ListDir(ctx, d.path)
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
	filesv, err := d.fs.ListDir(ctx, d.path)
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

func NewLocalWriter(name string) io.WriteSeeker {
		panic("ah")
}

func (f *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	err := f.fs.PrepareForRead(ctx, f.path, f.etag, NewLocalWriter(f.file.Name()), uint64(req.Offset), uint64(req.Size), nil)
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

func StartMount(mountpoint string, filesystem *FS) (*fuse.Conn, *fs.Server, chan struct{}) {
	c, err := fuse.Mount(
		mountpoint,
		fuse.FSName("pliantfuse"),
		fuse.Subtype("pliant"),
		fuse.LocalVolume(),
		fuse.VolumeName("pliant"),
	)
	if err != nil {
		log.Fatal(err)
	}

	doneChan := make(chan struct{})
	log.Println("Before server.Serve")
	server := fs.New(c, nil)
	go (func() {
		err := server.Serve(filesystem)
		if err != nil {
			log.Fatal(err)
		}
		close(doneChan)
	})()
	log.Println("After server.Serve")

	return c, server, doneChan
}

func tileRegion(region *Region, size uint64) []*Region {
	start := (region.Offset / size) * size
	end := ((region.Offset + region.Length + size - 1) / size)
	tiles := make([]*Region, 0, int((end-start)/size))
	for i := start; i < end; i += size {
		tiles = append(tiles, &Region{Offset: i, Length: size})
	}

	return tiles
}
