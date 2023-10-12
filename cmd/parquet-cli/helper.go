package main

import (
	"fmt"
	"os"

	"github.com/parquet-go/parquet-go"
)

func openParquetFile(filename string) (*parquet.File, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("unable to open file '%s': %w", filename, err)
	}

	info, err := file.Stat()
	if err != nil {
		return nil, fmt.Errorf("unable to get stat for '%s': %w", filename, err)
	}

	pfile, err := parquet.OpenFile(file, info.Size())
	if err != nil {
		return nil, fmt.Errorf("unable to read file '%s': %w", filename, err)
	}

	return pfile, nil
}
