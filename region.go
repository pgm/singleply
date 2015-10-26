package singleply

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
