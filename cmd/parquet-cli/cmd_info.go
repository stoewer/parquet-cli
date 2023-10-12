package main

import (
	"fmt"
	"os"

	"github.com/parquet-go/parquet-go"

	"github.com/stoewer/parquet-cli/pkg/inspect"
	"github.com/stoewer/parquet-cli/pkg/output"
)

type info struct {
	outputOptions
	File string `arg:""`
}

func (i *info) Run() error {
	file, err := os.Open(i.File)
	if err != nil {
		return fmt.Errorf("unable to open file '%s': %w", i.File, err)
	}

	stat, err := file.Stat()
	if err != nil {
		return fmt.Errorf("unable to get stat for '%s': %w", i.File, err)
	}

	pfile, err := parquet.OpenFile(file, stat.Size())
	if err != nil {
		return fmt.Errorf("unable to read file '%s': %w", i.File, err)
	}

	fileInfo, err := inspect.NewFileInfo(file, pfile)
	if err != nil {
		return fmt.Errorf("unable to read file info for '%s': %w", i.File, err)
	}

	return output.PrintTable(os.Stdout, i.Output, fileInfo)
}
