package main

import (
	"errors"

	"github.com/alecthomas/kong"
)

var cli struct {
	Schema   schema   `cmd:"" help:"Print the files schema"`
	ColStats colStats `cmd:"" help:"Show column numbers and statistics from a file"`
	RowStats rowStats `cmd:"" help:"Show statistics about each row in a file"`
}

type outputOptions struct {
	Output string `short:"o" optional:"" default:"json"`
}

func (o *outputOptions) Validate() error {
	if o.Output != "json" && o.Output != "tab" && o.Output != "csv" {
		return errors.New("output is expected to be 'json', 'tab', or 'csv'")
	}
	return nil
}

func main() {
	ctx := kong.Parse(
		&cli,
		kong.Name("parquet-cli"),
		kong.Description("A tool to analyze the schema and content of parquet files"),
		kong.UsageOnError(),
	)
	err := ctx.Run()
	ctx.FatalIfErrorf(err)
}
