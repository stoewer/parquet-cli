package main

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"

	"github.com/parquet-go/parquet-go/format"
	"github.com/segmentio/encoding/thrift"
	"github.com/stoewer/parquet-cli/pkg/output"
)

type schema struct {
	outputOptions
	File string `arg:""`
}

func (s *schema) Run() error {
	f, err := os.Open(s.File)
	if err != nil {
		return err
	}

	fi, err := f.Stat()
	if err != nil {
		return err
	}

	metadata, err := readMetadata(f, fi.Size())
	if err != nil {
		return err

	}

	return output.PrintTable(os.Stdout, s.Output, newMetadataTable(metadata))
}

// borrowed with love from github.com/segmentio/parquet-go/file.go:OpenFile()
func readMetadata(r io.ReaderAt, size int64) (*format.FileMetaData, error) {
	b := make([]byte, 8)

	if _, err := r.ReadAt(b[:4], 0); err != nil {
		return nil, fmt.Errorf("reading magic header of parquet file: %w", err)
	}
	if string(b[:4]) != "PAR1" {
		return nil, fmt.Errorf("invalid magic header of parquet file: %q", b[:4])
	}
	if n, err := r.ReadAt(b[:8], size-8); n != 8 {
		return nil, fmt.Errorf("reading magic footer of parquet file: %w", err)
	}
	if string(b[4:8]) != "PAR1" {
		return nil, fmt.Errorf("invalid magic footer of parquet file: %q", b[4:8])
	}

	footerSize := int64(binary.LittleEndian.Uint32(b[:4]))
	footerData := make([]byte, footerSize)
	if _, err := r.ReadAt(footerData, size-(footerSize+8)); err != nil {
		return nil, fmt.Errorf("reading footer of parquet file: %w", err)
	}

	protocol := thrift.CompactProtocol{}
	metadata := &format.FileMetaData{}
	if err := thrift.Unmarshal(&protocol, footerData, metadata); err != nil {
		return nil, fmt.Errorf("reading parquet file metadata: %w", err)
	}
	if len(metadata.Schema) == 0 {
		return nil, errors.New("missing root column")
	}

	return metadata, nil
}

type metadataTable struct {
	schema []format.SchemaElement
	row    int
}

func newMetadataTable(m *format.FileMetaData) *metadataTable {
	return &metadataTable{
		schema: m.Schema,
	}
}

func (t *metadataTable) Header() []any {
	return []any{
		"Type",
		"TypeLength",
		"RepetitionType",
		"Name",
		"NumChildren",
		"ConvertedType",
		"Scale",
		"Precision",
		"FieldID",
		"LogicalType",
	}
}

func (t *metadataTable) NextRow() (output.TableRow, error) {
	if t.row >= len(t.schema) {
		return nil, io.EOF
	}

	r := newMetadataRow(0, &t.schema[t.row])
	t.row++

	return r, nil
}

type metadataRow struct {
	n int
	s *format.SchemaElement
}

func newMetadataRow(n int, s *format.SchemaElement) *metadataRow {
	return &metadataRow{
		n: n,
		s: s,
	}
}

func (r *metadataRow) Row() int {
	return r.n
}

func (r *metadataRow) Cells() []any {
	return []any{
		r.s.Type,
		r.s.TypeLength,
		r.s.RepetitionType,
		r.s.Name,
		r.s.NumChildren,
		r.s.ConvertedType,
		r.s.Scale,
		r.s.Precision,
		r.s.FieldID,
		r.s.LogicalType,
	}
}

func (r *metadataRow) SerializableData() any {
	return r.s
}
