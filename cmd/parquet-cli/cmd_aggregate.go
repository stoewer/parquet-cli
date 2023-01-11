package main

import (
	"os"

	"github.com/stoewer/parquet-cli/pkg/inspect"
	"github.com/stoewer/parquet-cli/pkg/output"
)

type aggregate struct {
	outputOptions
	File    string `arg:""`
	GroupBy int    `short:"g" optional:"" help:"Aggregate stats grouped by the values of this column"`
	Columns []int  `short:"c" optional:"" help:"Restrict the output to the following columns (default: all possible columns)"`
}

func (rs *aggregate) Run() error {
	file, err := openParquetFile(rs.File)
	if err != nil {
		return err
	}

	options := inspect.AggregateOptions{
		Columns:       rs.Columns,
		GroupByColumn: rs.GroupBy,
	}

	rowStats, err := inspect.NewAggregateCalculator(file, options)
	if err != nil {
		return err
	}

	return output.Print(os.Stdout, rs.Output, rowStats)
}
