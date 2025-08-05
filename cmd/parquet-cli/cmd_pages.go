package main

import (
	"os"

	"github.com/stoewer/parquet-cli/pkg/inspect"
	"github.com/stoewer/parquet-cli/pkg/output"
)

type pages struct {
	outputOptions
	File   string `arg:""`
	Column int    `short:"c" required:"" help:"Read pages for the given column"`
	Limit  *int64 `optional:"" help:"Limit the output to the given number of pages"`
	Offset int64  `optional:"" help:"Begin the output at this page offset"`
}

func (p *pages) Run() error {
	file, err := openParquetFile(p.File)
	if err != nil {
		return err
	}

	options := inspect.PageInfoOptions{
		Column: p.Column,
		Pagination: inspect.Pagination{
			Limit:  p.Limit,
			Offset: p.Offset,
		},
	}

	pageInfo, err := inspect.NewPageInfo(file, options)
	if err != nil {
		return err
	}

	return output.Print(os.Stdout, pageInfo, &output.PrintOptions{Format: p.Output})
}
