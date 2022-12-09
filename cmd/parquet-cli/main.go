package main

import (
	"github.com/alecthomas/kong"
	"github.com/stoewer/parquet-cli/pkg/output"
)

var cli struct {
	Schema   schema   `cmd:"" help:"Print the files schema"`
	ColStats colStats `cmd:"" help:"Show column numbers and statistics from a file"`
	RowStats rowStats `cmd:"" help:"Show statistics about each row in a file"`
	Dump     dump     `cmd:"" help:"Print the content of the file"`
}

type outputOptions struct {
	Output output.Format `short:"o" optional:"" default:"tab"`
}

func (o *outputOptions) Validate() error {
	return o.Output.Validate()
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
