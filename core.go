package singleply

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strings"
	"sync"

	"errors"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"golang.org/x/net/context"
)

var InvalidPathError error = errors.New("Invalid path")

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

type FS struct {
	connector    Connector
	cache        Cache
	tracker      *Tracker
	stats        *Stats
	requestQueue chan *Req
	blockSize    uint64
}

func NewFileSystem(connector Connector, cache Cache, tracker *Tracker, stats *Stats, workers int, blockSize uint64) *FS {
	queue := make(chan *Req)
	for i := 0; i < workers; i++ {
		go WorkerLoop(stats, i, connector, queue)
	}
	return &FS{connector: connector, cache: cache, tracker: tracker, stats: stats, requestQueue: queue, blockSize: blockSize}
}

func (f *FS) Root() (fs.Node, error) {
	return &Dir{path: "", fs: f, childrenInUse: make(map[string]fs.Node)}, nil
}

func (f *FS) Invalidate(ctx context.Context, server *fs.Server, path string) error {
	if path[0] != '/' {
		return errors.New("Path must start with slash")
	}

	node, err := f.Root()
	//fmt.Printf("node=%s, err=%s\n", node, err)
	if err != nil {
		return err
	}

	if path != "/" {
		// drop leading slash
		path = path[1:]
		components := strings.Split(path, "/")
		//fmt.Printf("components=%s, node=%s, err=%s\n", components, node, err)
		for _, component := range components {
			dir, isDir := node.(*Dir)
			if !isDir {
				return InvalidPathError
			}
			nextNode, err := dir.lookupInUse(ctx, component)
			if err != nil {
				return err
			}
			node = nextNode
		}
	}

	err = server.InvalidateNodeData(node)
	//fmt.Printf("node=%s\n", node, err)
	return err
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
	req      *Req
	err      error
}

type Req struct {
	ctx    context.Context
	path   string
	etag   string
	writer io.WriteSeeker
	offset uint64
	length uint64
	status *State

	// upon completion either an error will be sent back or channel closed
	response chan *Resp
}

func WorkerLoop(stats *Stats, index int, connector Connector, queue chan *Req) {
	for {
		fmt.Printf("Worker %d waiting\n", index)

		req, ok := <-queue
		if !ok {
			fmt.Printf("!ok, killing worker %d\n", index)
			break
		}

		fmt.Printf("Worker %d handling %s %d +%d\n", index, req.path, req.offset, req.length)
		stats.RecordConnectorReadLen(req.length)
		prepared, err := connector.PrepareForRead(req.ctx, req.path, req.etag, req.writer, req.offset, req.length, req.status)
		fmt.Printf("Worker %d completed handling %s %d +%d\n", index, req.path, req.offset, req.length)
		req.response <- &Resp{prepared: prepared, req: req, err: err}
	}
}

func getMissingRegions(cache Cache, path string, offset uint64, length uint64, fileLength uint64, blockSize uint64) []*Region {
	tryCount := 0

	regionEnd := offset + length
	missing := make([]*Region, 0, 100)
	for {
		tryCount++
		if tryCount > 100 {
			panic("something is wrong")
		}
		fmt.Printf("offset=%d, regionEnd=%d, fileLength=%d\n", offset, regionEnd, fileLength)
		next := cache.GetFirstMissingRegion(path, offset, regionEnd-offset)
		if next == nil {
			break
		}

		// divide missing into chunk sized pieces
		for start := (next.Offset / blockSize) * blockSize; start < next.Offset+next.Length; start += blockSize {
			end := start + blockSize
			if end > fileLength {
				end = fileLength
			}
			r := &Region{start, end - start}
			fmt.Printf("Adding region %d +%d\n", r.Offset, r.Length)
			missing = append(missing, r)
			offset = end
		}
		if offset >= regionEnd {
			break
		}
	}

	return missing
}

func (fs *FS) PrepareForRead(ctx context.Context, path string, etag string, writer io.WriteSeeker, offset uint64, length uint64, fileLength uint64, status StatusCallback) error {
	fs.stats.RecordReadRequestLen(length)

	regions := getMissingRegions(fs.cache, path, offset, length, fileLength, fs.blockSize)
	if len(regions) == 0 {
		return nil
	}

	responses := make(chan *Resp)
	for _, region := range regions {
		fmt.Printf("Fetching region %s to fulfill read of (offset: %d, len: %s) for %s\n", region, offset, length, path)
		state := fs.tracker.AddOperation(fmt.Sprintf("PrepareForRead(%s, %d, %d)", path, region.Offset, region.Length))
		fs.requestQueue <- &Req{ctx: ctx, path: path, etag: etag, writer: writer, offset: region.Offset, length: region.Length, status: state, response: responses}
	}

	// block until all workers have responded
	var finalErr error = nil
	addedRegions := make([]*Region, 0, 100)
	for i := 0; i < len(regions); i++ {
		resp := <-responses
		fmt.Printf("Received %d out of %d responses\n", i, len(regions))

		fs.tracker.OperationComplete(resp.req.status)
		if resp.err != nil {
			fmt.Printf("error = %s\n", resp.err.Error())
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
		fs.cache.AddedRegions(path, prepared.Offset, prepared.Length)
	}

	return finalErr
}

type FileHandle struct {
	path string
	fs   *FS
	file *os.File
	etag string
	size uint64
}

type Dir struct {
	path   string
	fs     *FS
	parent *Dir

	lock          sync.Mutex
	childrenInUse map[string]fs.Node
}

type File struct {
	path   string
	fs     *FS
	size   uint64
	etag   string
	parent *Dir
}

func (d *Dir) Attr(ctx context.Context, a *fuse.Attr) error {
	a.Mode = os.ModeDir | 0777
	fmt.Printf("Dir.Attr(%s) -> %o\n", d.path, a.Mode)
	return nil
}

func (d *Dir) lookupInUse(ctx context.Context, name string) (fs.Node, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	node, hasNode := d.childrenInUse[name]
	if !hasNode {
		return nil, fuse.ErrNotCached
	}

	return node, nil
}

func (d *Dir) Lookup(ctx context.Context, name string) (fs.Node, error) {
	d.lock.Lock()
	defer d.lock.Unlock()

	node, hasNode := d.childrenInUse[name]
	if !hasNode {
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
			node = &Dir{parent: d, path: childName, fs: d.fs, childrenInUse: make(map[string]fs.Node)}
		} else {
			node = &File{parent: d, path: childName, fs: d.fs, size: entry.Size, etag: entry.Etag}
		}
		d.childrenInUse[name] = node
	}

	return node, nil
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

func (f *File) Forget() {
	if f.parent == nil {
		return
	}
	name := path.Base(f.path)
	f.parent.lock.Lock()
	defer f.parent.lock.Unlock()
	delete(f.parent.childrenInUse, name)
}

func (f *Dir) Forget() {
	if f.parent == nil {
		return
	}
	name := path.Base(f.path)
	f.parent.lock.Lock()
	defer f.parent.lock.Unlock()
	delete(f.parent.childrenInUse, name)
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

	return &FileHandle{path: f.path, fs: f.fs, file: localFile, etag: f.etag, size: f.size}, nil
}

type LocalWriter struct {
	name   string
	offset int64
}

func NewLocalWriter(name string) io.WriteSeeker {
	return &LocalWriter{name: name, offset: 0}
}

func (w *LocalWriter) Seek(offset int64, whence int) (int64, error) {
	if whence != 0 {
		panic("unsupported")
	}
	w.offset = offset
	return offset, nil
}

func (w *LocalWriter) Write(p []byte) (n int, err error) {
	fd, err := os.OpenFile(w.name, os.O_RDWR, 0777)
	if err != nil {
		return 0, err
	}
	defer fd.Close()

	_, err = fd.Seek(w.offset, 0)
	if err != nil {
		return 0, err
	}
	//	fmt.Printf("write offset=%d, len(p)=%d, buffer=% x\n", w.offset, len(p), p)
	n, err = fd.Write(p)
	w.offset += int64(n)
	return n, err
}

func (f *FileHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	fmt.Printf("(Read) Writing to %s\n", f.file.Name())
	lw := NewLocalWriter(f.file.Name())
	err := f.fs.PrepareForRead(ctx, f.path, f.etag, lw, uint64(req.Offset), uint64(req.Size), f.size, nil)
	if err != nil {
		fmt.Printf("PrepareForRead failed: %s\n", err.Error())
		return err
	}

	buffer := make([]byte, req.Size)
	n, err := f.file.ReadAt(buffer, req.Offset)
	if err != nil {
		fmt.Printf("ReadAt returned error: %s\n", err.Error())
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
