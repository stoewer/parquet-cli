package main

import (
	"github.com/stoewer/parquet-cli/pkg/inspect"
	"os"

	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"

	"github.com/stoewer/parquet-cli/pkg/output"
)

type info struct {
	outputOptions
	File string `arg:""`
}

func (i *info) Run() error {
	file, err := os.Open(i.File)
	if err != nil {
		return errors.Wrapf(err, "unable to open file '%s'", i.File)
	}

	stat, err := file.Stat()
	if err != nil {
		return errors.Wrapf(err, "unable to get stat for '%s'", i.File)
	}

	pfile, err := parquet.OpenFile(file, stat.Size())
	if err != nil {
		return errors.Wrapf(err, "unable to read file '%s'", i.File)
	}

	fileInfo, err := inspect.NewFileInfo(file, pfile)
	if err != nil {
		return errors.Wrapf(err, "unable to read file info for '%s'", i.File)
	}

	return output.PrintTable(os.Stdout, i.Output, fileInfo)
}
