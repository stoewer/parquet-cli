package main

import "fmt"

type rowStats struct {
	outputOptions
	File    string `arg:""`
	Columns []int  `short:"c" optional:"" help:"Restrict the Output to the following columns"`
}

func (rs *rowStats) Run() error {
	fmt.Println("Sub command row-stats not implemented yet")
	return nil
}
