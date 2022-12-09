package main

import (
	"errors"
	"os"

	"github.com/stoewer/parquet-cli/pkg/inspect"
	"github.com/stoewer/parquet-cli/pkg/output"
)

type dump struct {
	outputOptions
	File    string `arg:""`
	Columns []int  `short:"c" optional:"" help:"Restrict the output to the following columns"`
	Limit   *int64 `optional:"" help:"Limit the output to the given number of rows"`
	Offset  int64  `optional:"" help:"Begin the output at this row offset"`
}

func (d *dump) Run() error {
	if d.Output == output.FormatJSON {
		return errors.New("JSON output not supported for dump command")
	}

	file, err := openParquetFile(d.File)
	if err != nil {
		return err
	}

	options := inspect.RowStatOptions{
		SelectedCols: d.Columns,
		Pagination: inspect.Pagination{
			Limit:  d.Limit,
			Offset: d.Offset,
		},
	}

	rowDump, err := inspect.NewRowDump(file, options)
	if err != nil {
		return err
	}

	return output.Print(os.Stdout, d.Output, rowDump)
}
