package main

import (
	"os"

	"github.com/stoewer/parquet-cli/pkg/inspect"
	"github.com/stoewer/parquet-cli/pkg/output"
)

type rowStats struct {
	outputOptions
	File    string `arg:""`
	Columns []int  `short:"c" optional:"" help:"Restrict the output to the following columns"`
	Limit   *int64 `optional:"" help:"Limit the output to the given number of rows"`
	Offset  int64  `optional:"" help:"Begin the output at this row offset"`
}

func (rs *rowStats) Run() error {
	file, err := openParquetFile(rs.File)
	if err != nil {
		return err
	}

	options := inspect.RowStatOptions{
		SelectedCols: rs.Columns,
		Pagination: inspect.Pagination{
			Limit:  rs.Limit,
			Offset: rs.Offset,
		},
	}

	rowStats, err := inspect.NewRowStatCalculator(file, options)
	if err != nil {
		return err
	}

	return output.Print(os.Stdout, rs.Output, rowStats)
}
