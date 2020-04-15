package sparss

import (
	"fmt"
	"strings"
	"testing"
)

func TestCompressor_Run(t *testing.T) {
	const x = DefaultEmptyValue

	tests := []struct {
		rowLen     int
		emptyValue int
		origTable  []int
	}{
		{
			rowLen:     3,
			emptyValue: x,
			origTable: []int{
				1, x, x,
				x, 1, x,
				x, x, 1,
			},
		},
		{
			rowLen:     3,
			emptyValue: x,
			origTable: []int{
				1, x, x,
				1, x, x,
				1, x, x,
			},
		},
		{
			rowLen:     3,
			emptyValue: x,
			origTable: []int{
				1, x, x,
				1, 1, x,
				1, 1, 1,
			},
		},
		{
			rowLen:     3,
			emptyValue: x,
			origTable: []int{
				1, 1, 1,
				1, 1, 1,
				1, 1, 1,
			},
		},
		{
			rowLen:     3,
			emptyValue: x,
			origTable: []int{
				x, x, x,
				x, x, x,
				x, x, x,
			},
		},
		{
			rowLen:     3,
			emptyValue: x,
			origTable: []int{
				x, 1, x,
				x, x, x,
				x, 1, 1,
			},
		},
		{
			rowLen:     3,
			emptyValue: -1,
			origTable: []int{
				0, 1, -1,
				-1, -1, 0,
				-1, -1, 1,
			},
		},
	}
	for i, tt := range tests {
		t.Run(fmt.Sprintf("#%v", i), func(t *testing.T) {
			t.Logf("empty value: %v", tt.emptyValue)
			t.Logf("orig (length: %v): %+v", len(tt.origTable), prettyUp(tt.origTable, tt.rowLen, tt.emptyValue))

			comp, err := NewRDCompressor()
			if err != nil {
				t.Fatalf("failed to call NewRDCompressor(); error: %v", err)
			}
			table, err := NewTable(tt.origTable, tt.rowLen, EmptyValue(tt.emptyValue))
			if err != nil {
				t.Fatalf("failed to call NewTable(); error: %v", err)
			}
			result, err := comp.Compress(table)
			if err != nil {
				t.Fatalf("failed to Compress(); error: %v", err)
			}
			t.Logf("result (length: %v): %+v", len(result.Entries), prettyUp(result.Entries, x, tt.emptyValue))
			t.Logf("bounds (length: %v): %+v", len(result.Bounds), prettyUp(result.Bounds, x, ForbiddenValue))
			t.Logf("row displacement (length: %v): %+v", len(result.RowDisplacement), result.RowDisplacement)

			for i, expected := range tt.origTable {
				row := i / tt.rowLen
				col := i % tt.rowLen
				actual, err := result.Lookup(row, col)
				if err != nil {
					t.Errorf("failed to call Lookup(); error: %v", err)
				}
				if actual != expected {
					t.Errorf("invalid entry; row: %v, col: %v, want: %v, got: %v", row, col, expected, actual)
				}
			}
		})
	}

	t.Run("When Lookup() is called whith invalid index, it returns an error", func(t *testing.T) {
		comp, err := NewRDCompressor()
		if err != nil {
			t.Fatalf("failed to call NewRDCompressor(); error: %v", err)
		}
		table, err := NewTable([]int{
			x, 1, x,
			1, x, x,
			x, 1, 1,
		}, 3)
		if err != nil {
			t.Fatalf("failed to call NewTable(); error: %v", err)
		}
		result, err := comp.Compress(table)
		if err != nil {
			t.Fatalf("failed to call Run(); error: %v", err)
		}
		invalidIndexes := []struct {
			row int
			col int
		}{
			{
				row: 0,
				col: -1,
			},
			{
				row: -1,
				col: 0,
			},
			{
				row: 2,
				col: 3,
			},
			{
				row: 3,
				col: 2,
			},
		}
		for _, ii := range invalidIndexes {
			t.Logf("row: %v, col: %v", ii.row, ii.col)

			e, err := result.Lookup(ii.row, ii.col)
			if err == nil {
				t.Errorf("Lookup() must return an error")
				continue
			}
			if e != x {
				t.Errorf("Lookup() must return the empty value; want: %v, got: %v", x, e)
			}
		}
	})
}

func prettyUp(target []int, rowLen int, emptyValue int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "[")
	for i, v := range target {
		if i > 0 {
			if rowLen > 0 && i%rowLen == 0 {
				fmt.Fprintf(&b, " | ")
			} else {
				fmt.Fprintf(&b, " ")
			}
		}

		if v == emptyValue {
			fmt.Fprintf(&b, "_")
		} else {
			fmt.Fprintf(&b, "%v", v)
		}
	}
	fmt.Fprintf(&b, "]")

	return b.String()
}
