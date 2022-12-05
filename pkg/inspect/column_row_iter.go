package inspect

import (
	"io"

	"github.com/pkg/errors"
	"github.com/segmentio/parquet-go"
)

func newColumnRowIterator(column *parquet.Column) (*columnRowIterator, error) {
	pages := column.Pages()
	page, err := pages.ReadPage()
	if err != nil {
		return nil, errors.Wrap(err, "unable to create row iterator")
	}
	values := page.Values()

	return &columnRowIterator{
		column:       column,
		pages:        pages,
		values:       values,
		readBuffer:   make([]parquet.Value, 1000),
		resultBuffer: make([]parquet.Value, 1000),
	}, nil
}

type columnRowIterator struct {
	column       *parquet.Column
	pages        parquet.Pages
	values       parquet.ValueReader
	readBuffer   []parquet.Value
	resultBuffer []parquet.Value
	unread       []parquet.Value
}

func (r *columnRowIterator) ColumnName() string {
	return r.column.Name()
}

func (r *columnRowIterator) NextRow() ([]parquet.Value, error) {
	result := r.resultBuffer[:0]

	// consume unread values
	for i, v := range r.unread {
		if isNewRow(&v) && len(result) > 0 {
			r.unread = r.unread[i:]
			return result, nil
		}
		result = append(result, v)
	}
	r.unread = nil

	// read more values
	for {
		newValues := r.readBuffer[:]
		count, err := r.values.ReadValues(newValues)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, errors.Wrapf(err, "unable to read row values from column '%s'", r.column.Name())
		}

		for i := 0; i < count; i++ {
			v := newValues[i]
			if isNewRow(&v) && len(result) > 0 {
				r.unread = newValues[i:count]
				return result, nil
			}
			result = append(result, v)
		}

		if errors.Is(err, io.EOF) {
			p, err := r.pages.ReadPage()
			if err != nil {
				if errors.Is(err, io.EOF) {
					if len(result) > 0 {
						return result, nil
					}
					return nil, err
				}
				return nil, errors.Wrapf(err, "unable to read new page from column '%s'", r.column.Name())
			}

			r.values = p.Values()
		}
	}
}

func isNewRow(v *parquet.Value) bool {
	return v.RepetitionLevel() == 0
}
