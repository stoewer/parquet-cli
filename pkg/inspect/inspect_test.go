package inspect

import (
	"testing"

	tf "github.com/stoewer/parquet-cli/pkg/testfile"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLeafColumns(t *testing.T) {
	data := []tf.Nested{{
		ColA: 1,
		ColB: []tf.Inner{{
			InnerA: "a",
			Map: []tf.InnerMap{{
				Key: "aa",
				Val: ptr(11),
			}},
		}},
	}}

	_, pfile := tf.Open(t, tf.New(t, data))
	columns := LeafColumns(pfile)
	require.Len(t, columns, 5)
	assert.Equal(t, "ColA", columns[0].Name())
	assert.Equal(t, "InnerA", columns[1].Name())
	assert.Equal(t, "Key", columns[2].Name())
	assert.Equal(t, "Val", columns[3].Name())
	assert.Equal(t, "ColC", columns[4].Name())
}
