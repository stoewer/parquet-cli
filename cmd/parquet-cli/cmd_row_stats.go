package main

import (
	"os"

	"github.com/stoewer/parquet-cli/pkg/inspect"
	"github.com/stoewer/parquet-cli/pkg/output"
)

type rowStats struct {
	outputOptions
	File    string `arg:""`
	Columns []int  `short:"c" optional:"" help:"Restrict the Output to the following columns"`
}

func (rs *rowStats) Run() error {
	file, err := openParquetFile(rs.File)
	if err != nil {
		return err
	}

	rowStats, err := inspect.NewRowStatCalculator(file, rs.Columns)
	if err != nil {
		return err
	}

	return output.Print(os.Stdout, rs.Output, rowStats)
}
