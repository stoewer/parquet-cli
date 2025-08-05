package inspect

import (
	"errors"
	"fmt"
	"io"

	"github.com/stoewer/parquet-cli/pkg/output"

	"github.com/parquet-go/parquet-go"
)

type PageInfoOptions struct {
	Pagination
	Column int
}

func NewPageInfo(file *parquet.File, opt PageInfoOptions) (*PageInfo, error) {
	// fmt.Println(file.Schema())
	fmt.Println(opt)

	all := LeafColumns(file)
	if opt.Column < 0 || opt.Column >= len(all) {
		return nil, fmt.Errorf("column index expectd between 0 and %d, but was %d", len(all)-1, opt.Column)
	}

	// select row groups according to offset
	rowGroups := file.RowGroups()
	var (
		currRowGroup int
		currPage     int64
	)
	for currRowGroup < len(rowGroups) {
		ci, err := rowGroups[currRowGroup].ColumnChunks()[opt.Column].ColumnIndex()
		if err != nil {
			return nil, err
		}

		if currPage+int64(ci.NumPages()) > opt.Offset {
			break
		}

		currPage += int64(ci.NumPages())
		currRowGroup++
	}

	if currRowGroup >= len(rowGroups) {
		return nil, errors.New("no row groups / pages left")
	}

	// forward to the correct page
	pages := rowGroups[0].ColumnChunks()[opt.Column].Pages()
	for currPage < opt.Offset {
		currPage++
		_, err := pages.ReadPage()
		if err != nil {
			return nil, err
		}
	}

	return &PageInfo{
		Pagination:   opt.Pagination,
		column:       opt.Column,
		rowGroups:    rowGroups,
		pages:        pages,
		currRowGroup: currRowGroup,
		currPage:     currPage,
	}, nil
}

type PageInfo struct {
	Pagination
	column       int
	rowGroups    []parquet.RowGroup
	pages        parquet.Pages
	currRowGroup int
	currPage     int64
}

func (p *PageInfo) Header() []any {
	return []any{"Row group", "Page", "Compressed size", "Rows", "Values", "Nulls", "Min val", "Max val"}
}

func (p *PageInfo) NextRow() (output.TableRow, error) {
	if p.currRowGroup >= len(p.rowGroups) || (p.Limit != nil && p.currPage >= p.Offset+*p.Limit) {
		return nil, io.EOF
	}

	page, err := p.pages.ReadPage()
	if err != nil {
		if !errors.Is(err, io.EOF) {
			return nil, err
		}

		p.currRowGroup++
		if p.currRowGroup >= len(p.rowGroups) {
			return nil, io.EOF
		}

		p.pages = p.rowGroups[p.currRowGroup].ColumnChunks()[p.column].Pages()
		return p.NextRow()
	}

	p.currPage++
	minVal, maxVal, ok := page.Bounds()

	pl := PageLine{
		RowGroup:       p.currRowGroup,
		Page:           p.currPage,
		CompressedSize: page.Size(),
		NumRows:        page.NumRows(),
		NumValues:      page.NumValues(),
		NumNulls:       page.NumNulls(),
	}
	if ok {
		pl.MinVal = truncateString(minVal.String(), 25)
		pl.MaxVal = truncateString(maxVal.String(), 25)
	}

	return &pl, nil
}

type PageLine struct {
	RowGroup       int    `json:"row_group"`
	Page           int64  `json:"page"`
	CompressedSize int64  `json:"compressed_size"`
	NumRows        int64  `json:"num_rows"`
	NumValues      int64  `json:"num_values"`
	NumNulls       int64  `json:"num_nulls"`
	MinVal         string `json:"min_val"`
	MaxVal         string `json:"max_val"`
}

func (p *PageLine) Cells() []any {
	return []any{p.RowGroup, p.Page, p.CompressedSize, p.NumRows, p.NumValues, p.NumNulls, p.MinVal, p.MaxVal}
}

func (p *PageLine) SerializableData() any {
	return p
}

func truncateString(s string, max int) string {
	if len(s) <= max {
		return s
	}
	return s[:max] + "..."
}
