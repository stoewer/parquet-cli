package main

import "fmt"

type schema struct {
	File string `arg:""`
}

func (s *schema) Run() error {
	fmt.Println("Sub command schema not implemented yet")
	return nil
}
