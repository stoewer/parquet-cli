package output

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"

	"github.com/pkg/errors"
)

type Format string

const (
	FormatJSON = "json"
	FormatCSV  = "csv"
	FormatTab  = "tab"
)

func (f *Format) Validate() error {
	switch *f {
	case FormatJSON, FormatCSV, FormatTab:
		return nil
	default:
		return errors.New("output format is expected to be 'json', 'tab', or 'csv'")
	}
}

type Table interface {
	Header() []interface{}
	NextRow() (TableRow, error)
}

type TableRow interface {
	Row() int
	Cells() []interface{}
	Data() interface{}
}

func Print(w io.Writer, f Format, data Table) error {
	switch f {
	case FormatJSON:
		return printJSON(w, data)
	case FormatTab:
		return printTable(w, data)
	default:
		return errors.Errorf("format not supported yet '%s'", f)
	}
}

func printTable(w io.Writer, data Table) error {
	tw := tabwriter.NewWriter(w, 0, 0, 2, ' ', 0)

	formatBuilder := strings.Builder{}
	for range data.Header() {
		formatBuilder.WriteString("%v\t")
	}
	formatBuilder.WriteRune('\n')
	format := formatBuilder.String()

	_, err := fmt.Fprintf(tw, format, data.Header()...)
	if err != nil {
		return err
	}

	var count int
	row, err := data.NextRow()
	for err == nil {
		_, err = fmt.Fprintf(tw, format, row.Cells()...)
		if err != nil {
			return err
		}
		if count > 0 && count%10 == 0 {
			err = tw.Flush()
			if err != nil {
				return err
			}
		}

		count++
		row, err = data.NextRow()
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	return tw.Flush()
}

func printJSON(w io.Writer, data Table) error {
	fmt.Println("[")

	var count int
	buf := bytes.NewBuffer(make([]byte, 1024))
	row, err := data.NextRow()

	for err == nil {
		if count > 0 {
			_, err = fmt.Fprint(w, ",\n   ")
		} else {
			_, err = fmt.Fprint(w, "   ")
		}
		if err != nil {
			return err
		}

		buf.Reset()
		err = json.NewEncoder(buf).Encode(row.Data())
		if err != nil {
			return err
		}
		buf.Truncate(buf.Len() - 1) // remove the newline

		_, err = fmt.Fprint(w, buf)
		if err != nil {
			return err
		}

		count++
		row, err = data.NextRow()
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	fmt.Println("\n]")
	return nil
}
