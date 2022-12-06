package testfile

import (
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/require"

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

func New[T any](t testing.TB, data []T) string {
	t.Helper()

	// creat temporary file
	file, err := os.CreateTemp("", tempPattern)
	require.NoError(t, err)
	name := file.Name()

	defer file.Close()
	t.Cleanup(func() {
		_ = os.Remove(name)
	})

	// write file
	writer := parquet.NewGenericWriter[T](file)
	defer writer.Close()

	var count int
	for count < len(data) {
		c, err := writer.Write(data)
		require.NoError(t, err)
		count += c
	}

	return name
}

func Open(t testing.TB, name string) *parquet.File {
	t.Helper()

	file, err := os.Open(name)
	require.NoError(t, err)
	t.Cleanup(func() {
		_ = file.Close()
	})

	info, err := file.Stat()
	require.NoError(t, err)

	pfile, err := parquet.OpenFile(file, info.Size())
	require.NoError(t, err)

	return pfile
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
