package main

import (
	"fmt"

	"github.com/alecthomas/kong"
	"github.com/stoewer/parquet-cli/pkg/output"
)

var (
	Version = "n.a."
	Commit  = "n.a."
)

var cli struct {
	Version   version   `cmd:"" help:"Print the parquet-cli version"`
	Schema    schema    `cmd:"" help:"Print the files schema"`
	ColStats  colStats  `cmd:"" help:"Show column numbers and statistics from a file"`
	RowStats  rowStats  `cmd:"" help:"Show statistics about each row in a file"`
	Aggregate aggregate `cmd:"" help:"Show aggregate statistics grouped by values in another column"`
	Dump      dump      `cmd:"" help:"Print the content of the file"`
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

type version struct{}

func (v version) Run() error {
	fmt.Printf("parquet-cli: version=%s commit=%s\n", Version, Commit)
	return nil
}
