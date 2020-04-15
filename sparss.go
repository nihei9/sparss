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

type Table struct {
	entries      []int
	numOfEntries int
	numOfRows    int
	numOfCols    int
	rowLen       int
	emptyEntry   int
}

func NewTable(entries []int, rowLen int, options ...TableOption) (*Table, error) {
	if len(entries) <= 0 {
		return nil, fmt.Errorf("len(entries) must be >= 1")
	}
	if rowLen <= 0 {
		return nil, fmt.Errorf("rowLen must be >=1")
	}
	if len(entries)%rowLen != 0 {
		return nil, fmt.Errorf("len(entries) %% rowLen must be 0")
	}

	numOfRows := len(entries) / rowLen
	numOfCols := len(entries) / numOfRows
	t := &Table{
		entries:      entries,
		numOfEntries: len(entries),
		numOfRows:    numOfRows,
		numOfCols:    numOfCols,
		rowLen:       rowLen,
		emptyEntry:   DefaultEmptyEntry,
	}

	for _, option := range options {
		err := option(t)
		if err != nil {
			return nil, fmt.Errorf("failed to apply TableOption functions to a Table object; error: %w", err)
		}
	}

	return t, nil
}

type TableOption func(t *Table) error

func EmptyEntry(e int) TableOption {
	return func(t *Table) error {
		t.emptyEntry = e
		return nil
	}
}

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
}

func NewRDCompressor() (*RDCompressor, error) {
	return &RDCompressor{}, nil
}

type rowInfo struct {
	rowNum        int
	nonEmptyCount int
	nonEmptyCol   []int
}

func (c *RDCompressor) Compress(origTable *Table) (*RDResult, error) {
	if origTable.numOfEntries <= 0 {
		return nil, fmt.Errorf("table is empty")
	}

	rowInfo := make([]rowInfo, origTable.numOfRows)
	{
		row := 0
		col := 0
		rowInfo[0].rowNum = 0
		for _, v := range origTable.entries {
			if col == origTable.rowLen {
				row++
				col = 0
				rowInfo[row].rowNum = row
			}
			if v != origTable.emptyEntry {
				rowInfo[row].nonEmptyCount++
				rowInfo[row].nonEmptyCol = append(rowInfo[row].nonEmptyCol, col)
			}
			col++
		}

		sort.SliceStable(rowInfo, func(i int, j int) bool {
			return rowInfo[i].nonEmptyCount > rowInfo[j].nonEmptyCount
		})
	}

	entries := make([]int, origTable.numOfEntries)
	bounds := make([]int, origTable.numOfEntries)
	resultBottom := origTable.rowLen
	rowDisplacement := make([]int, origTable.numOfRows)
	{
		for i := 0; i < origTable.numOfEntries; i++ {
			entries[i] = origTable.emptyEntry
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
					if entries[nextRowDisplacement+col] == origTable.emptyEntry {
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
					entries[nextRowDisplacement+col] = origTable.entries[(rInfo.rowNum*origTable.rowLen)+col]
					bounds[nextRowDisplacement+col] = rInfo.rowNum
				}
				resultBottom = nextRowDisplacement + origTable.rowLen
				nextRowDisplacement++
				break
			}
		}
	}

	result := &RDResult{
		OrigNumOfRows:   origTable.numOfRows,
		OrigNumOfCols:   origTable.numOfCols,
		EmptyEntry:      origTable.emptyEntry,
		Entries:         entries[0:resultBottom],
		Bounds:          bounds[0:resultBottom],
		RowDisplacement: rowDisplacement,
	}

	return result, nil
}
