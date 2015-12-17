package singleply

import (
	"bytes"
	"fmt"
	. "gopkg.in/check.v1"
)

type RegionSetSuite struct {
}

var _ = Suite(&RegionSetSuite{})
var _ = fmt.Sprintf("hello!")

func asStr(regions []Region) string {
	bb := make([]byte, 0)
	b := bytes.NewBuffer(bb)
	for _, r := range regions {
		fmt.Fprintf(b, "(%d, %d)", r.Offset, r.Length)
	}
	return string(b.Bytes())
}
func (s *RegionSetSuite) TestCloneWithMut(c *C) {
	regions := make([]Region, 3)
	regions[0] = Region{10, 10}
	regions[1] = Region{30, 10}
	regions[2] = Region{50, 10}

	result := cloneWithReplacement(regions, 0, 0, Region{30, 1})
	c.Assert(asStr(result), Equals, "(30, 1)(10, 10)(30, 10)(50, 10)")

	result = cloneWithReplacement(regions, 0, 1, Region{30, 1})
	c.Assert(asStr(result), Equals, "(30, 1)(30, 10)(50, 10)")

	result = cloneWithReplacement(regions, 1, 1, Region{30, 1})
	c.Assert(asStr(result), Equals, "(10, 10)(30, 1)(30, 10)(50, 10)")

	result = cloneWithReplacement(regions, 1, 2, Region{30, 1})
	c.Assert(asStr(result), Equals, "(10, 10)(30, 1)(50, 10)")

	result = cloneWithReplacement(regions, 2, 2, Region{30, 1})
	c.Assert(asStr(result), Equals, "(10, 10)(30, 10)(30, 1)(50, 10)")

	result = cloneWithReplacement(regions, 3, 3, Region{30, 1})
	c.Assert(asStr(result), Equals, "(10, 10)(30, 10)(50, 10)(30, 1)")

	result = cloneWithReplacement(regions, 0, 3, Region{30, 1})
	c.Assert(asStr(result), Equals, "(30, 1)")
}

func (s *RegionSetSuite) TestSequentalRegionAdds(c *C) {
	rs := NewRegionSet()
	full := rs.firstMissing(Region{0, 100})
	c.Assert(full.Offset, Equals, uint64(0))
	c.Assert(full.Length, Equals, uint64(100))

	rs.add(Region{0, 10})
	after10 := rs.firstMissing(Region{0, 100})
	c.Assert(after10.Offset, Equals, uint64(10))
	c.Assert(after10.Length, Equals, uint64(90))

	c.Assert(len(rs.Regions), Equals, 1)
	r := rs.Regions[0]
	c.Assert(r.Offset, Equals, uint64(0))
	c.Assert(r.Length, Equals, uint64(10))

	rs.add(Region{10, 10})
	fmt.Printf("After add: %s\n", asStr(rs.Regions))
	c.Assert(len(rs.Regions), Equals, 1)
	r = rs.Regions[0]
	c.Assert(r.Offset, Equals, uint64(0))
	c.Assert(r.Length, Equals, uint64(20))
	after20 := rs.firstMissing(Region{0, 100})
	c.Assert(after20.Offset, Equals, uint64(20))
	c.Assert(after20.Length, Equals, uint64(80))

	rs.add(Region{20, 80})
	shouldBeNil := rs.firstMissing(Region{0, 100})
	c.Assert(shouldBeNil, IsNil)

	c.Assert(len(rs.Regions), Equals, 1)
	r = rs.Regions[0]
	c.Assert(r.Offset, Equals, uint64(0))
	c.Assert(r.Length, Equals, uint64(100))
}

func (s *RegionSetSuite) TestOutOfOrderRegionAdds(c *C) {
	rs := NewRegionSet()

	rs.add(Region{10, 10})
	//	fmt.Printf("regions: %s\n", asStr(rs.Regions))
	c.Assert(len(rs.Regions), Equals, 1)

	rs.add(Region{30, 70})
	//	fmt.Printf("regions: %s\n", asStr(rs.Regions))
	c.Assert(len(rs.Regions), Equals, 2)

	rs.add(Region{20, 10})
	//	fmt.Printf("regions: %s\n", asStr(rs.Regions))
	c.Assert(len(rs.Regions), Equals, 1)

	rs.add(Region{0, 10})
	//	fmt.Printf("regions: %s\n", asStr(rs.Regions))
	c.Assert(len(rs.Regions), Equals, 1)

	shouldBeNil := rs.firstMissing(Region{0, 100})
	c.Assert(shouldBeNil, IsNil)

	c.Assert(len(rs.Regions), Equals, 1)
	r := rs.Regions[0]
	c.Assert(r.Offset, Equals, uint64(0))
	c.Assert(r.Length, Equals, uint64(100))
}

func (s *RegionSetSuite) TestOverlappingRegionAdds(c *C) {
	rs := NewRegionSet()

	rs.add(Region{0, 10})
	//	fmt.Printf("regions: %s\n", asStr(rs.Regions))
	rs.add(Region{30, 70})
	//	fmt.Printf("regions: %s\n", asStr(rs.Regions))
	rs.add(Region{9, 40})
	//	fmt.Printf("regions: %s\n", asStr(rs.Regions))

	shouldBeNil := rs.firstMissing(Region{0, 100})
	c.Assert(shouldBeNil, IsNil)

	c.Assert(len(rs.Regions), Equals, 1)
	r := rs.Regions[0]
	c.Assert(r.Offset, Equals, uint64(0))
	c.Assert(r.Length, Equals, uint64(100))
}
