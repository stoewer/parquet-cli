package main

import (
	"os"

	"github.com/stoewer/parquet-cli/pkg/inspect"
	"github.com/stoewer/parquet-cli/pkg/output"
)

type colStats struct {
	outputOptions
	Verbose bool   `short:"v" optional:"" help:"Print additional information"`
	File    string `arg:""`
	Columns []int  `short:"c" optional:"" help:"Restrict the output to the following columns"`
}

func (cs *colStats) Run() error {
	file, err := openParquetFile(cs.File)
	if err != nil {
		return err
	}

	stats, err := inspect.NewColStatCalculator(file, cs.Columns, cs.Verbose)
	if err != nil {
		return err
	}

	return output.Print(os.Stdout, stats, &output.PrintOptions{Format: cs.Output})
}
