package inspect

import (
	"testing"

	"github.com/stoewer/parquet-cli/pkg/output"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	tf "github.com/stoewer/parquet-cli/pkg/testfile"
)

func TestFileInfo(t *testing.T) {
	filename := tf.New(t, testDataNested)

	file, pfile := tf.Open(t, filename)
	fileInfo, err := NewFileInfo(file, pfile)
	require.NoError(t, err)

	row, err := fileInfo.NextRow()
	checkFileInfoRow(t, row, err, "Name", nil)
	row, err = fileInfo.NextRow()
	checkFileInfoRow(t, row, err, "Size", 1353)
	row, err = fileInfo.NextRow()
	checkFileInfoRow(t, row, err, "Footer", 612)
	row, err = fileInfo.NextRow()
	checkFileInfoRow(t, row, err, "Version", int32(2))
	row, err = fileInfo.NextRow()
	checkFileInfoRow(t, row, err, "Creator", "github.com/parquet-go/parquet-go")
	row, err = fileInfo.NextRow()
	checkFileInfoRow(t, row, err, "Rows", 3)
	row, err = fileInfo.NextRow()
	checkFileInfoRow(t, row, err, "RowGroups", 1)
	row, err = fileInfo.NextRow()
	checkFileInfoRow(t, row, err, "Columns", 5)
}

func checkFileInfoRow(t *testing.T, row output.TableRow, err error, key string, val any) {
	require.NoError(t, err)
	cells := row.Cells()
	require.Len(t, cells, 2)
	assert.Equal(t, key, cells[0])
	if val != nil {
		assert.EqualValues(t, val, cells[1])
	}
}
