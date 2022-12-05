package inspect

import "github.com/segmentio/parquet-go"

func LeafColumns(col *parquet.Column) []*parquet.Column {
	if col.Leaf() {
		return []*parquet.Column{col}
	}

	leafs := make([]*parquet.Column, 0, len(col.Columns()))
	for _, child := range col.Columns() {
		leafs = append(leafs, LeafColumns(child)...)
	}

	return leafs
}
