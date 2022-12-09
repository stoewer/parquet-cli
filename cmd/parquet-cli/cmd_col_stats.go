package main

import (
	"os"

	"github.com/stoewer/parquet-cli/pkg/inspect"
	"github.com/stoewer/parquet-cli/pkg/output"
)

type colStats struct {
	outputOptions
	File    string `arg:""`
	Columns []int  `short:"c" optional:"" help:"Restrict the output to the following columns"`
}

func (cs *colStats) Run() error {
	file, err := openParquetFile(cs.File)
	if err != nil {
		return err
	}

	rowStats, err := inspect.NewColStatCalculator(file, cs.Columns)
	if err != nil {
		return err
	}

	return output.Print(os.Stdout, cs.Output, rowStats)
}
