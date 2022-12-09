package inspect

import (
	"io"

	"github.com/pkg/errors"
	"github.com/segmentio/parquet-go"
	"github.com/stoewer/parquet-cli/pkg/output"
)

var (
	columnStatHeader = [...]interface{}{"Index", "Name", "Size", "Pages", "Rows", "Values", "Nulls"}
)

type ColumnStats struct {
	Index  int    `json:"index"`
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	Pages  int    `json:"pages"`
	Rows   int64  `json:"rows"`
	Values int64  `json:"values"`
	Nulls  int64  `json:"nulls"`

	cells []interface{}
}

func (rs *ColumnStats) Row() int {
	return rs.Index
}

func (rs *ColumnStats) Data() interface{} {
	return rs
}

func (rs *ColumnStats) Cells() []interface{} {
	if rs.cells == nil {
		rs.cells = []interface{}{
			rs.Index,
			rs.Name,
			rs.Size,
			rs.Pages,
			rs.Rows,
			rs.Values,
			rs.Nulls,
		}
	}
	return rs.cells
}

func NewColStatCalculator(file *parquet.File, selectedCols []int) (*ColStatCalculator, error) {
	all := LeafColumns(file)
	var columns []*parquet.Column

	if len(selectedCols) == 0 {
		columns = all
	} else {
		columns = make([]*parquet.Column, 0, len(selectedCols))
		for _, idx := range selectedCols {
			if idx >= len(all) {
				return nil, errors.Errorf("column index expectd be below %d but was %d", idx, len(all))
			}
			columns = append(columns, all[idx])
		}
	}

	return &ColStatCalculator{columns: columns}, nil
}

type ColStatCalculator struct {
	columns []*parquet.Column
	current int
}

func (cc *ColStatCalculator) Header() []interface{} {
	return columnStatHeader[:]
}

func (cc *ColStatCalculator) NextRow() (output.TableRow, error) {
	if cc.current >= len(cc.columns) {
		return nil, errors.Wrapf(io.EOF, "stop iteration: no more culumns")
	}

	col := cc.columns[cc.current]
	cc.current++

	stats := ColumnStats{Index: col.Index(), Name: col.Name()}
	pages := col.Pages()

	page, err := pages.ReadPage()
	for err == nil {
		stats.Pages++
		stats.Size += page.Size()
		stats.Rows += page.NumRows()
		stats.Values += page.NumValues()
		stats.Nulls += page.NumNulls()
		page, err = pages.ReadPage()
	}
	if !errors.Is(err, io.EOF) {
		return nil, errors.Wrapf(err, "unable to read page rom column '%s", col.Name())
	}

	return &stats, nil
}
