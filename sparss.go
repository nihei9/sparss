package sparss

import (
	"fmt"
	"sort"
)

const (
	DefaultEmptyEntry = int(0)
	ForbiddenEntry    = maxInt
	MaxBoundsIndex    = maxInt - 1

	maxUInt = ^uint(0)
	maxInt  = int(maxUInt >> 1)
)

type RDResult struct {
	OrigNumOfRows   int
	OrigNumOfCols   int
	EmptyEntry      int
	Entries         []int
	Bounds          []int
	RowDisplacement []int
}

func (r *RDResult) Lookup(row int, col int) (int, error) {
	if row < 0 || row >= r.OrigNumOfRows || col < 0 || col >= r.OrigNumOfCols {
		err := fmt.Errorf("out of range; table size: %vx%v, accessed: (%v, %v)", r.OrigNumOfRows, r.OrigNumOfCols, row, col)
		return r.EmptyEntry, err
	}
	if r.Bounds[r.RowDisplacement[row]+col] != row {
		return r.EmptyEntry, nil
	}
	return r.Entries[r.RowDisplacement[row]+col], nil
}

type RDCompressor struct {
	emptyEntry int
}

type RDCompressorOption func(c *RDCompressor)

func EmptyEntry(e int) RDCompressorOption {
	return func(c *RDCompressor) {
		c.emptyEntry = e
	}
}

func NewRDCompressor(options ...RDCompressorOption) (*RDCompressor, error) {
	c := &RDCompressor{
		emptyEntry: DefaultEmptyEntry,
	}
	for _, option := range options {
		option(c)
	}

	return c, nil
}

type rowInfo struct {
	rowNum        int
	nonEmptyCount int
	nonEmptyCol   []int
}

func (c *RDCompressor) Compress(origTable []int, rowLen int) (*RDResult, error) {
	if len(origTable) <= 0 {
		return nil, fmt.Errorf("len(origTable) must be >= 1")
	}
	if rowLen <= 0 {
		return nil, fmt.Errorf("rowLen must be >=1")
	}
	if len(origTable)%rowLen != 0 {
		return nil, fmt.Errorf("len(origTable) %% rowLen must be 0")
	}

	numOfRows := len(origTable) / rowLen
	rowInfo := make([]rowInfo, numOfRows)
	{
		row := 0
		col := 0
		rowInfo[0].rowNum = 0
		for _, v := range origTable {
			if col == rowLen {
				row++
				col = 0
				rowInfo[row].rowNum = row
			}
			if v != c.emptyEntry {
				rowInfo[row].nonEmptyCount++
				rowInfo[row].nonEmptyCol = append(rowInfo[row].nonEmptyCol, col)
			}
			col++
		}

		sort.SliceStable(rowInfo, func(i int, j int) bool {
			return rowInfo[i].nonEmptyCount > rowInfo[j].nonEmptyCount
		})
	}

	entries := make([]int, len(origTable))
	bounds := make([]int, len(origTable))
	resultBottom := rowLen
	rowDisplacement := make([]int, numOfRows)
	{
		for i := 0; i < len(origTable); i++ {
			entries[i] = c.emptyEntry
			bounds[i] = ForbiddenEntry
		}

		nextRowDisplacement := 0
		for _, rInfo := range rowInfo {
			if rInfo.nonEmptyCount <= 0 {
				continue
			}

			for {
				isOverlapped := false
				for _, col := range rInfo.nonEmptyCol {
					if entries[nextRowDisplacement+col] == c.emptyEntry {
						continue
					}
					nextRowDisplacement++
					isOverlapped = true
					break
				}
				if isOverlapped {
					continue
				}

				rowDisplacement[rInfo.rowNum] = nextRowDisplacement
				for _, col := range rInfo.nonEmptyCol {
					entries[nextRowDisplacement+col] = origTable[(rInfo.rowNum*rowLen)+col]
					bounds[nextRowDisplacement+col] = rInfo.rowNum
				}
				resultBottom = nextRowDisplacement + rowLen
				nextRowDisplacement++
				break
			}
		}
	}

	result := &RDResult{
		OrigNumOfRows:   numOfRows,
		OrigNumOfCols:   len(origTable) / numOfRows,
		EmptyEntry:      c.emptyEntry,
		Entries:         entries[0:resultBottom],
		Bounds:          bounds[0:resultBottom],
		RowDisplacement: rowDisplacement,
	}

	return result, nil
}
