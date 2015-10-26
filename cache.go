package singleply

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"sync"
	"time"

	"github.com/boltdb/bolt"
)

type FileCacheEntry struct {
	LocalPath string
	Valid     *RegionSet
}

type RegionSet struct {
	Regions []Region
}

// dumb implementation
func (rs *RegionSet) add(region Region) {
	rs.Regions = append(rs.Regions, region)
}

func (rs *RegionSet) firstMissing(region Region) *Region {
	remainder := &region

	for _, r := range rs.Regions {
		remainder = remainder.FirstNonOverlap(&r)
		if remainder == nil {
			return nil
		}
	}

	return remainder
}

// func (rs *RegionSet) add(region Region) {
// 	start := sort.Search(len(rs.regions), func(i int) bool {
// 		return rs.regions[i].offset >= region.offset
// 	})
//
// 	end := sort.Search(len(rs.regions), func(i int) bool {
// 		return rs.regions[i].offset >= region.offset + region.length
// 	})
//
// 	// adjust region by overlapping region at start and end if there is one and then
// 	// splice the new region into the array
//
// 	// if the previous region overlaps this one, merge them
// 	if rs.regions[i-1].offset + rs.regions[i-1].length > region.offset {
// 		i -= 1
// 		rs.regions[i].length = region.offset + region.length - rs.regions[i].offset
// 	} else {
// 		// insert as a new item
// 		// TODO
// 	}
//
// }

type DirEntries []*FileStat

func (f *DirEntries) Get(name string) *FileStat {
	panic("unimp")
}

const FILE_MAP = "files"
const DIR_MAP = "dirs"

type Cache interface {
	GetLocalFile(path string, length uint64) string
	GetFirstMissingRegion(path string, offset uint64, length uint64) *Region
	AddedRegions(path string, offset uint64, length uint64)

	GetListDir(path string) *DirEntries
	PutListDir(path string, files *DirEntries)
}

type LocalCache struct {
	rootDir string
	lock    sync.Mutex
	db      *bolt.DB
}

func NewLocalCache(rootDir string) (*LocalCache, error) {
	dbFilename := rootDir + "/db"
	db, err := bolt.Open(dbFilename, 0600, &bolt.Options{Timeout: 5 * time.Second})
	if err != nil {
		return nil, err
	}

	err = db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(FILE_MAP))
		if err != nil {
			return err
		}
		_, err = tx.CreateBucketIfNotExists([]byte(DIR_MAP))
		return err
	})

	if err != nil {
		return nil, err
	}

	return &LocalCache{rootDir: rootDir, db: db}, nil
}

func (c *LocalCache) GetLocalFile(path string, length uint64) (string, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var localPath string
	localPath = ""

	err := c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(FILE_MAP))
		key := []byte(path)
		entryBytes := b.Get(key)
		if entryBytes == nil {
			var err error
			localPath, err = ioutil.TempDir(c.rootDir, "l")
			if err != nil {
				return err
			}

			buffer := bytes.NewBuffer(make([]byte, 0, 100))
			enc := gob.NewEncoder(buffer)
			e := &FileCacheEntry{LocalPath: localPath, Valid: &RegionSet{Regions: make([]Region, 0)}}
			err = enc.Encode(e)
			if err != nil {
				return err
			}

			bb := buffer.Bytes()
			b.Put(key, bb)

			fmt.Printf("e=%s, writing %s -> len(): %d\n", e, key, len(bb))
		} else {
			var e FileCacheEntry

			buffer := bytes.NewBuffer(entryBytes)
			dec := gob.NewDecoder(buffer)
			err := dec.Decode(&e)
			if err != nil {
				return err
			}

			localPath = e.LocalPath
		}
		return nil
	})

	return localPath, err
}

func (c *LocalCache) GetFirstMissingRegion(path string, offset uint64, length uint64) *Region {
	c.lock.Lock()
	defer c.lock.Unlock()

	var missing *Region

	err := c.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(FILE_MAP))
		key := []byte(path)
		entryBytes := b.Get(key)
		fmt.Printf("fetched len %d for \"%s\"\n", len(entryBytes), path)
		var e FileCacheEntry

		buffer := bytes.NewBuffer(entryBytes)
		dec := gob.NewDecoder(buffer)
		err := dec.Decode(&e)
		if err != nil {
			return err
		}

		missing = e.Valid.firstMissing(Region{offset, length})

		return nil
	})

	if err != nil {
		panic(err.Error())
	}

	return missing
}

func (c *LocalCache) AddedRegions(path string, offset uint64, length uint64) {
	c.lock.Lock()
	defer c.lock.Unlock()

	c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(FILE_MAP))
		key := []byte(path)
		entryBytes := b.Get(key)
		var e FileCacheEntry

		buffer := bytes.NewBuffer(entryBytes)
		dec := gob.NewDecoder(buffer)
		dec.Decode(&e)

		e.Valid.add(Region{offset, length})

		buffer = bytes.NewBuffer(make([]byte, 0, 100))
		enc := gob.NewEncoder(buffer)
		enc.Encode(e)

		b.Put(key, buffer.Bytes())

		return nil
	})
}

func (c *LocalCache) GetListDir(path string) (*DirEntries, error) {
	c.lock.Lock()
	defer c.lock.Unlock()

	var files DirEntries
	found := false

	err := c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(DIR_MAP))
		key := []byte(path)
		value := b.Get(key)
		if value != nil {
			buffer := bytes.NewBuffer(value)
			dec := gob.NewDecoder(buffer)
			dec.Decode(&files)
			found = true
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	if !found {
		return nil, nil
	}

	return &files, nil
}

func (c *LocalCache) PutListDir(path string, files *DirEntries) error {
	buffer := bytes.NewBuffer(make([]byte, 0, 100))
	enc := gob.NewEncoder(buffer)
	enc.Encode(files)

	c.lock.Lock()
	defer c.lock.Unlock()

	err := c.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(DIR_MAP))
		key := []byte(path)
		b.Put(key, buffer.Bytes())
		return nil
	})

	return err
}
