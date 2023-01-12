package inspect

import (
	"fmt"
	"io"
	"testing"

	"github.com/segmentio/parquet-go"
	tf "github.com/stoewer/parquet-cli/pkg/testfile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testDataNested = []tf.Nested{
	{
		ColA: 1,
		ColB: []tf.Inner{
			{InnerA: "a", Map: []tf.InnerMap{{Key: "aa", Val: ptr(11)}, {Key: "bb"}}},
			{InnerA: "b", Map: []tf.InnerMap{{Key: "cc", Val: ptr(33)}}},
		},
		ColC: "aaa",
	},
	{
		ColA: 2,
		ColB: []tf.Inner{
			{InnerA: "c"},
		},
		ColC: "bbb",
	},
	{
		ColA: 3,
		ColB: []tf.Inner{
			{InnerA: "d", Map: []tf.InnerMap{{Key: "dd", Val: ptr(44)}}},
			{InnerA: "e", Map: []tf.InnerMap{{Key: "ee"}, {Key: "ff", Val: ptr(66)}}},
		},
		ColC: "ccc",
	},
}

func TestGroupingColumnIterator_NextGroup(t *testing.T) {
	tests := []struct {
		column   int
		groupBy  *int
		limit    *int64
		offset   int64
		expected [][]string
	}{
		{
			column:   1,
			expected: [][]string{{"a", "b"}, {"c"}, {"d", "e"}},
		},
		{
			column:   2,
			expected: [][]string{{"aa", "bb", "cc"}, {""}, {"dd", "ee", "ff"}},
		},
		{
			column:   2,
			offset:   1,
			expected: [][]string{{""}, {"dd", "ee", "ff"}},
		},
		{
			column:   1,
			offset:   1,
			limit:    ptr(int64(1)),
			expected: [][]string{{"c"}},
		},
		{
			column:   2,
			groupBy:  ptr(1),
			expected: [][]string{{"aa", "bb"}, {"cc"}, {""}, {"dd"}, {"ee", "ff"}},
		},
		{
			column:   1,
			groupBy:  ptr(1),
			expected: [][]string{{"a"}, {"b"}, {"c"}, {"d"}, {"e"}},
		},
	}

	filename := tf.New(t, testDataNested)

	for _, tt := range tests {
		var l int64
		if tt.limit != nil {
			l = *tt.limit
		}

		t.Run(fmt.Sprintf("col %d limit %d offset %d", tt.column, l, tt.offset), func(t *testing.T) {
			file := tf.Open(t, filename)
			columns := LeafColumns(file)

			var groupByColumn *parquet.Column
			if tt.groupBy != nil {
				groupByColumn = columns[*tt.groupBy]
			}

			group, err := newGroupingColumnIterator(columns[tt.column], groupByColumn, Pagination{Limit: tt.limit, Offset: tt.offset})
			require.NoError(t, err)

			rowsStr := groupsToString(t, group)
			assert.Equal(t, tt.expected, rowsStr)
		})
	}
}

var globalGroup []parquet.Value

func BenchmarkGroupingColumnIterator_NextGroup(b *testing.B) {
	filename := tf.New(b, tf.RandomNested(100_000, 10))
	file := tf.Open(b, filename)
	cols := LeafColumns(file)
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		rows1, _ := newGroupingColumnIterator(cols[1], nil, Pagination{})
		r, err := rows1.NextGroup()
		for err == nil {
			r, err = rows1.NextGroup()
		}
		globalGroup = r
	}
}

func groupsToString(t *testing.T, groups *groupingColumnIterator) [][]string {
	var result [][]string
	group, err := groups.NextGroup()
	for err == nil {
		var groupStr []string
		for _, val := range group {
			if val.IsNull() {
				groupStr = append(groupStr, "")
			} else {
				groupStr = append(groupStr, val.String())
			}
		}
		result = append(result, groupStr)
		group, err = groups.NextGroup()
	}
	require.ErrorIs(t, err, io.EOF)
	return result
}

func ptr[T any](v T) *T {
	return &v
}
