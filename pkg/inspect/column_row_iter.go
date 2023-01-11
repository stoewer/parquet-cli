package inspect

import (
	"io"

	"github.com/pkg/errors"
	"github.com/segmentio/parquet-go"
)

func newColumnRowIterator(column, groupByColumn *parquet.Column, pagination Pagination) (*columnRowIterator, error) {
	it := columnRowIterator{
		column:        column,
		groupByColumn: groupByColumn,
		pages:         column.Pages(),
		readBuffer:    make([]parquet.Value, 1000),
		resultBuffer:  make([]parquet.Value, 1000),
		rowOffset:     pagination.Offset,
		rowLimit:      pagination.Limit,
	}
	err := it.forwardToOffset()
	if err != nil {
		return nil, errors.Wrap(err, "unable to create column row iterator")
	}

	return &it, err
}

type columnRowIterator struct {
	column        *parquet.Column
	groupByColumn *parquet.Column
	pages         parquet.Pages
	values        parquet.ValueReader
	readBuffer    []parquet.Value
	resultBuffer  []parquet.Value
	unread        []parquet.Value
	currentRow    int64
	rowOffset     int64
	rowLimit      *int64
}

func (r *columnRowIterator) Column() *parquet.Column {
	return r.column
}

func (r *columnRowIterator) NextRow() ([]parquet.Value, error) {
	result := r.resultBuffer[:0]

	for {
		for i, v := range r.unread {
			if r.rowLimit != nil && r.currentRow >= *r.rowLimit+r.rowOffset {
				return nil, errors.Wrapf(io.EOF, "stop iteration: row limit reached")
			}
			if r.isNewRow(&v) && len(result) > 0 {
				r.unread = r.unread[i:]
				r.currentRow++
				return result, nil
			}
			result = append(result, v)
		}

		count, err := r.values.ReadValues(r.readBuffer)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, errors.Wrapf(err, "unable to read values from column '%s'", r.column.Name())
		}

		r.unread = r.readBuffer[:count]
		if len(r.unread) > 0 {
			continue
		}

		if errors.Is(err, io.EOF) {
			p, err := r.pages.ReadPage()
			if err != nil {
				if !errors.Is(err, io.EOF) {
					return nil, errors.Wrapf(err, "unable to read new page from column '%s'", r.column.Name())
				}
				if len(result) > 0 {
					return result, nil
				}
				return nil, err
			}
			r.values = p.Values()
		}
	}
}

func (r *columnRowIterator) forwardToOffset() error {
	for {
		page, err := r.pages.ReadPage()
		if err != nil {
			if !errors.Is(err, io.EOF) {
				return errors.Wrapf(err, "unable to read new page from column '%s'", r.column.Name())
			}
			return err
		}

		if r.currentRow+page.NumRows() >= r.rowOffset {
			r.values = page.Values()
			break
		}

		r.currentRow += page.NumRows()
	}

	for {
		for i, v := range r.unread {
			if r.isNewRow(&v) && i > 0 {
				r.currentRow++
			}
			if r.currentRow >= r.rowOffset {
				r.unread = r.unread[i:]
				return nil
			}
		}

		count, err := r.values.ReadValues(r.readBuffer)
		if err != nil && !errors.Is(err, io.EOF) {
			return errors.Wrapf(err, "unable to read values from column '%s'", r.column.Name())
		}

		r.unread = r.readBuffer[:count]

		if errors.Is(err, io.EOF) && count <= 0 {
			return err
		}
	}
}

func (r *columnRowIterator) isNewRow(v *parquet.Value) bool {
	if r.groupByColumn == nil {
		// no group by column: each now row is a new group
		return v.RepetitionLevel() == 0
	}
	if r.column.Index() == r.groupByColumn.Index() {
		// column equals group by column: each new value is a new group
		return true
	}

	// a value belongs to new group when it repeats at the same level or
	// lower as the definition value of the group by column
	return v.RepetitionLevel() <= r.groupByColumn.MaxDefinitionLevel()
}
