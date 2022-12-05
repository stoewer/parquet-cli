package main

import "fmt"

type colStats struct {
	outputOptions
	File string `arg:""`
}

func (cs *colStats) Run() error {
	fmt.Println("Sub command col-stats not implemented yet")
	return nil
}
