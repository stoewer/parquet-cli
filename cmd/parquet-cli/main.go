package main

import (
	"github.com/alecthomas/kong"
	"github.com/grafana/parquet-cli/pkg/output"
)

var cli struct {
	Schema   schema   `cmd:"" help:"Print the files schema"`
	ColStats colStats `cmd:"" help:"Show column numbers and statistics from a file"`
	RowStats rowStats `cmd:"" help:"Show statistics about each row in a file"`
}

type outputOptions struct {
	// TODO try output format here
	Output string `short:"o" optional:"" default:"json"`
}

func (o *outputOptions) Validate() error {
	f := output.Format(o.Output)
	return f.Validate()
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
