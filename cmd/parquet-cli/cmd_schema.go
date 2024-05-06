package main

import (
	"os"

	"github.com/stoewer/parquet-cli/pkg/inspect"

	"github.com/stoewer/parquet-cli/pkg/output"
)

type schema struct {
	outputOptions
	File string `arg:""`
}

func (s *schema) Run() error {
	pf, err := openParquetFile(s.File)
	if err != nil {
		return err
	}

	sch := inspect.NewSchema(pf)
	return output.Print(os.Stdout, sch, &output.PrintOptions{Format: s.Output})
}
