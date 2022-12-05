package inspect

import (
	"fmt"
	"io"
	"testing"

	"github.com/grafana/parquet-cli/pkg/testfile"
	"github.com/segmentio/parquet-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestColumnRowIterator_NextRow(t *testing.T) {
	data := []testfile.Nested{
		{ColA: 1, ColB: []testfile.Inner{{InnerA: "a", InnerB: nil}, {InnerA: "b", InnerB: ptr("aa")}}},
		{ColA: 2, ColB: []testfile.Inner{{InnerA: "c", InnerB: ptr("bb")}}},
		{ColA: 3, ColB: []testfile.Inner{{InnerA: "d", InnerB: ptr("cc")}, {InnerA: "e", InnerB: ptr("dd")}}},
	}

	tests := []struct {
		columnIdx int
		expected  [][]string
	}{
		{
			columnIdx: 1,
			expected:  [][]string{{"a", "b"}, {"c"}, {"d", "e"}},
		},
		{
			columnIdx: 2,
			expected:  [][]string{{"", "aa"}, {"bb"}, {"cc", "dd"}},
		},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("column %d", tt.columnIdx), func(t *testing.T) {
			file, remove, err := testfile.New(data)
			require.NoError(t, err)
			t.Cleanup(remove)

			columns := LeafColumns(file.Root())
			rows, err := newColumnRowIterator(columns[tt.columnIdx])
			require.NoError(t, err)

			rowsStr := rowsToStr(t, rows)
			assert.Equal(t, tt.expected, rowsStr)
		})
	}
}

var globalRow []parquet.Value

func BenchmarkColumnRowIterator_NextRow(b *testing.B) {
	data := testfile.RandomNested(100_000, 100)
	file, cleanup, _ := testfile.New(data)
	cols := LeafColumns(file.Root())
	b.Cleanup(cleanup)
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		rows1, _ := newColumnRowIterator(cols[1])
		r, err := rows1.NextRow()
		for err == nil {
			r, err = rows1.NextRow()
		}
		globalRow = r
	}
}

func rowsToStr(t *testing.T, rows *columnRowIterator) [][]string {
	var result [][]string
	row, err := rows.NextRow()
	for err == nil {
		var rowStr []string
		for _, val := range row {
			if val.IsNull() {
				rowStr = append(rowStr, "")
			} else {
				rowStr = append(rowStr, val.String())
			}
		}
		result = append(result, rowStr)
		row, err = rows.NextRow()
	}
	require.ErrorIs(t, err, io.EOF)
	return result
}

func ptr[T any](v T) *T {
	return &v
}
