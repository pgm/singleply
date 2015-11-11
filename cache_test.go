package singleply

import (
	"fmt"
	"testing"

	. "gopkg.in/check.v1"
)

type CacheSuite struct{}

var _ = Suite(&CacheSuite{})
var _ = fmt.Sprintf("hello!")

func Test(t *testing.T) { TestingT(t) }

func (r *Region) str() string {
	return fmt.Sprintf("%d:%d", r.Offset, r.Length)
}

func (s *CacheSuite) TestRegionSet(c *C) {
	var rs RegionSet

	rs.add(Region{10, 20})

	fmt.Printf("---------\n")
	r4 := rs.firstMissing(Region{9, 21})
	c.Assert(r4.str(), Equals, "9:1")

	r2 := rs.firstMissing(Region{10, 21})
	c.Assert(r2.str(), Equals, "30:1")

	r1 := rs.firstMissing(Region{29, 2})
	c.Assert(r1.str(), Equals, "30:1")

	r3 := rs.firstMissing(Region{11, 20})
	c.Assert(r3.str(), Equals, "30:1")

}

func (s *CacheSuite) TestLocalFiles(c *C) {
	cache, err := NewLocalCache(c.MkDir())
	c.Assert(err, IsNil)

	local, err := cache.GetLocalFile("x/y/z", 100)
	c.Assert(local, Not(Equals), "")
	c.Assert(err, IsNil)

	// getting local file twice results in same file
	local2, err := cache.GetLocalFile("x/y/z", 100)
	c.Assert(local2, Equals, local)
	c.Assert(err, IsNil)

	region := cache.GetFirstMissingRegion("x/y/z", 10, 20)

	c.Assert(region.str(), Equals, "10:20")

	cache.AddedRegions("x/y/z", 10, 20)

	// full overlap
	region = cache.GetFirstMissingRegion("x/y/z", 10, 20)
	c.Assert(region, IsNil)

	// full overlap
	region = cache.GetFirstMissingRegion("x/y/z", 11, 8)
	c.Assert(region, IsNil)

	// one extra byte before
	region = cache.GetFirstMissingRegion("x/y/z", 9, 21)
	c.Assert(region.str(), Equals, "9:1")

	// one extra byte after
	region = cache.GetFirstMissingRegion("x/y/z", 10, 21)
	c.Assert(region.str(), Equals, "30:1")

	// an extra byte before and after
	region = cache.GetFirstMissingRegion("x/y/z", 9, 22)
	c.Assert(region.str(), Equals, "9:1")

	// register regions (10-30) and (40-60) as populated
	cache.AddedRegions("x/y/z", 40, 20)

	region = cache.GetFirstMissingRegion("x/y/z", 29, 12)
	c.Assert(region.str(), Equals, "30:10")
}

func (s *CacheSuite) TestDirOperations(c *C) {
	cache, err := NewLocalCache(c.MkDir())
	c.Assert(err, IsNil)

	dir, err := cache.GetListDir("a")
	c.Assert(err, IsNil)
	c.Assert(dir, IsNil)

	files := make([]*FileStat, 1)
	files[0] = &FileStat{Name: "b", IsDir: false, Size: 10}
	d := &DirEntries{Files: files}
	cache.PutListDir("a", d)

	dir2, err := cache.GetListDir("a")
	c.Assert(err, IsNil)
	c.Assert(len(dir2.Files), Equals, 1)
	c.Assert(dir2.Files[0], DeepEquals, files[0])
}
