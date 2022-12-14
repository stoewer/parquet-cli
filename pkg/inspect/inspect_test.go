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

	file := tf.Open(t, tf.New(t, data))
	columns := LeafColumns(file)
	require.Len(t, columns, 4)
	assert.Equal(t, "ColA", columns[0].Name())
	assert.Equal(t, "InnerA", columns[1].Name())
	assert.Equal(t, "Key", columns[2].Name())
	assert.Equal(t, "Val", columns[3].Name())
}
