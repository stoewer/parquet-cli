package testfile

import (
	"math/rand"
	"os"

	"github.com/segmentio/parquet-go"
)

const (
	tempPattern = "parquet-cli-testfile-*"
)

var (
	rnd     = rand.New(rand.NewSource(314159265359))
	letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	words   = []string{"antelope", "bear", "beaver", "cat", "cuttlefish", "dingo", "dog", "elephant", "emu", "fish",
		"gorilla", "hedgehog", "hippo", "horse", "jellyfish", "koala", "lion", "lynx", "meercat", "moose", "mouse",
		"narwhal", "parrot", "possum", "reindeer", "salamander", "shark", "tiger", "wolf", "yak"}
)

type Flat struct {
	ColA int
	ColB string
	ColC *string
}

type Nested struct {
	ColA int
	ColB []Inner
}

type Inner struct {
	InnerA string
	InnerB *string
}

func New[T any](data []T) (*parquet.File, func(), error) {
	file, err := os.CreateTemp("", tempPattern)
	if err != nil {
		return nil, nil, err
	}
	name := file.Name()

	// write file
	writer := parquet.NewGenericWriter[T](file)

	var count int
	for count < len(data) {
		c, err := writer.Write(data)
		if err != nil {
			writer.Close()
			file.Close()
			return nil, nil, err
		}
		count += c
	}
	writer.Close()
	file.Close()

	// open again as parquet file
	info, err := os.Stat(name)
	if err != nil {
		return nil, nil, err
	}
	file, err = os.Open(name)
	if err != nil {
		return nil, nil, err
	}

	pfile, err := parquet.OpenFile(file, info.Size())
	if err != nil {
		file.Close()
		return nil, nil, err
	}
	cleanup := func() {
		file.Close()
		_ = os.Remove(name)
	}

	return pfile, cleanup, nil
}

func RandomNested(rows, levels int) []Nested {
	var data []Nested
	for i := 0; i < rows; i++ {
		row := Nested{ColA: randomInt()}
		for j := 0; j < levels; j++ {
			inner := Inner{InnerA: randomStr(), InnerB: randomWord()}
			row.ColB = append(row.ColB, inner)
		}
		data = append(data, row)
	}
	return data
}

func randomInt() int {
	return rnd.Intn(1000) + 100
}

func randomStr() string {
	n := rnd.Intn(10) + 20
	s := make([]rune, n)
	for i := range s {
		s[i] = letters[rnd.Intn(len(letters))]
	}
	return string(s)
}

func randomWord() *string {
	return &words[rnd.Intn(len(words))]
}
