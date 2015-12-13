package singleply

import (
	"fmt"
)

type Region struct {
	Offset uint64
	Length uint64
}

type RegionSet struct {
	Regions []Region
}

func NewRegionSet() *RegionSet {
	return &RegionSet{Regions: make([]Region, 0)}
}

func (rs *RegionSet) findIndexContaining(offset uint64) (int, bool) {
	for i := 0 ; i < len(rs.Regions) ; i++ {
		if rs.Regions[i].Offset <= offset && (rs.Regions[i].Length + rs.Regions[i].Offset) >= offset {
			return i, true;
		}
		if rs.Regions[i].Offset > offset {
			return i, false;
		}
	}
	
	return len(rs.Regions), false
}

func cloneWithReplacement(regions []Region, startIndex int, stopIndex int, newRegion Region) []Region {
	fmt.Printf("cloneWithReplacement(..., %d, %d, %s)\n", startIndex, stopIndex, newRegion)
	l := make([]Region, 0, len(regions)+1)
	l = append(l, regions[:startIndex]...)
	l = append(l, newRegion)
	return append(l, regions[stopIndex:]...)
}

// dumb implementation
func (rs *RegionSet) add(region Region) {
	fmt.Printf("Adding %s\n", region)
	startIndex, containsStart := rs.findIndexContaining(region.Offset)
	fmt.Printf("startIndex=%d\n", startIndex)
	stop := region.Offset + region.Length

	if !containsStart {
		if(startIndex > 0) {
			prevStop := rs.Regions[startIndex-1].Offset + rs.Regions[startIndex-1].Length
			if prevStop + 1 >= region.Offset {
				// overlaps at the start, so take the offset from this region
				region.Offset = rs.Regions[startIndex-1].Offset
				region.Length = stop - region.Offset
				startIndex -= 1
				fmt.Printf("Overlap at start\n")
			} else {
				// gap between previous and this region
				//startIndex += 1
			}
		}
	} else {
		region.Offset = rs.Regions[startIndex].Offset
		region.Length = stop - region.Offset
	}
	
	stopIndex, containsStop := rs.findIndexContaining(stop)
	fmt.Printf("stopIndex = %d\n", stopIndex)
	if ! containsStop {
		if stopIndex+1 < len(rs.Regions) {
			fmt.Printf("stop = %d, rs.Regions[stopIndex].Offset=%d\n", stop, rs.Regions[stopIndex+1].Offset)
			if stop >= rs.Regions[stopIndex+1].Offset {
				// overlaps at the end, so take the length to the end
				fmt.Printf("Overlap at end, so taking length to the end\n")
				region.Length = (rs.Regions[stopIndex+1].Offset + rs.Regions[stopIndex+1].Length) - region.Offset
				stopIndex += 1
			} else {
			}
		}
	} else {
		region.Length = (rs.Regions[stopIndex].Offset + rs.Regions[stopIndex].Length) - region.Offset
		stopIndex += 1
	}
	
	rs.Regions = cloneWithReplacement(rs.Regions, startIndex, stopIndex, region)
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

func (r *Region) FirstNonOverlap(r2 *Region) *Region {
	overlap := r.Intersect(r2)
	if overlap == nil {
		return r
	}

	if overlap.Offset == r.Offset {
		start := overlap.Offset + overlap.Length
		length := r.Offset + r.Length - start
		if length == 0 {
			return nil
		}
		return &Region{start, length}
	} else {
		end := overlap.Offset
		length := end - r.Offset
		if length == 0 {
			return nil
		}
		return &Region{r.Offset, length}
	}
}

func max(a uint64, b uint64) uint64 {
	if a > b {
		return a
	} else {
		return b
	}
}

func min(a uint64, b uint64) uint64 {
	if a < b {
		return a
	} else {
		return b
	}
}

func (r *Region) Intersect(r2 *Region) *Region {
	start := max(r.Offset, r2.Offset)
	end := min(r.Offset+r.Length, r2.Offset+r2.Length)

	if end < start {
		return nil
	}

	return &Region{Offset: start, Length: end - start}
}

func (r *Region) Union(r2 *Region) *Region {
	start := min(r.Offset, r2.Offset)
	end := max(r.Offset+r.Length, r2.Offset+r2.Length)

	return &Region{Offset: start, Length: end - start}
}
