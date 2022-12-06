package inspect

import (
	"fmt"

	"github.com/pkg/errors"
	"github.com/segmentio/parquet-go"
	"github.com/stoewer/parquet-cli/pkg/output"
)

var (
	cellFields = [3]string{"size", "values", "nulls"}
)

type CellStats struct {
	Column string `json:"col"`
	Size   int    `json:"size"`
	Values int    `json:"values"`
	Nulls  int    `json:"nulls"`
}

type RowStats struct {
	RowNumber int
	Stats     []CellStats
}

func (rs *RowStats) Row() int {
	return rs.RowNumber
}

func (rs *RowStats) Data() interface{} {
	return rs.Stats
}

func (rs *RowStats) Cells() []interface{} {
	cells := make([]interface{}, 0, len(rs.Stats)*len(cellFields)+1)
	cells = append(cells, rs.RowNumber)
	for _, c := range rs.Stats {
		cells = append(cells, c.Size, c.Values, c.Nulls)
	}
	return cells
}

type RowStatOptions struct {
	Pagination
	SelectedCols []int
}

func NewRowStatCalculator(file *parquet.File, options RowStatOptions) (*RowStatCalculator, error) {
	type indexedColumn struct {
		idx int
		col *parquet.Column
	}

	all := LeafColumns(file.Root())
	var columns []indexedColumn

	if len(options.SelectedCols) == 0 {
		columns = make([]indexedColumn, 0, len(all))
		for idx, col := range all {
			columns = append(columns, indexedColumn{idx: idx, col: col})
		}
	} else {
		columns = make([]indexedColumn, 0, len(options.SelectedCols))
		for _, idx := range options.SelectedCols {
			if idx >= len(all) {
				return nil, errors.Errorf("column index expectd be below %d but was %d", idx, len(all))
			}
			columns = append(columns, indexedColumn{idx: idx, col: all[idx]})
		}
	}

	c := RowStatCalculator{
		header:     make([]interface{}, 0, len(columns)*len(cellFields)+1),
		columnIter: make([]*columnRowIterator, 0, len(columns)),
	}

	c.header = append(c.header, "Row")
	for _, ic := range columns {
		it, err := newColumnRowIterator(ic.col, options.Pagination)
		if err != nil {
			return nil, errors.Wrapf(err, "unable to create row stats calculator")
		}
		c.columnIter = append(c.columnIter, it)
		c.header = append(c.header, fmt.Sprintf("%d/%s: %s", ic.idx, ic.col.Name(), cellFields[0]), cellFields[1], cellFields[2])
	}

	return &c, nil
}

type RowStatCalculator struct {
	header     []interface{}
	columnIter []*columnRowIterator
	rowNumber  int
}

func (c *RowStatCalculator) Header() []interface{} {
	return c.header
}

func (c *RowStatCalculator) NextRow() (output.TableRow, error) {
	rs := RowStats{
		RowNumber: c.rowNumber,
		Stats:     make([]CellStats, 0, len(c.columnIter)),
	}

	for _, it := range c.columnIter {
		values, err := it.NextRow()
		if err != nil {
			return nil, err
		}
		cs := CellStats{Column: it.ColumnName()}
		for _, val := range values {
			if val.IsNull() {
				cs.Nulls++
			} else {
				cs.Values++
				cs.Size += len(val.Bytes())
			}
		}
		rs.Stats = append(rs.Stats, cs)
	}

	c.rowNumber++
	return &rs, nil
}
