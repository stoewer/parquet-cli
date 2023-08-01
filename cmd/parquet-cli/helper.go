package main

import (
	"os"

	"github.com/parquet-go/parquet-go"
	"github.com/pkg/errors"
)

func openParquetFile(filename string) (*parquet.File, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to open file '%s'", filename)
	}

	info, err := file.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "unable to get stat for '%s'", filename)
	}

	pfile, err := parquet.OpenFile(file, info.Size())
	if err != nil {
		return nil, errors.Wrapf(err, "unable to read file '%s'", filename)
	}

	return pfile, nil
}
