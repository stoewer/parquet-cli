package output

import (
	"bytes"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"text/tabwriter"
)

// PrintTable writes the TableIterator data to w using the provided format.
func PrintTable(w io.Writer, f Format, data TableIterator) error {
	switch f {
	case FormatJSON:
		return printJSON(w, data)
	case FormatTab:
		return printTab(w, data)
	case FormatCSV:
		return printCSV(w, data)
	default:
		return fmt.Errorf("format not supported yet '%s'", f)
	}
}

func printTab(w io.Writer, data TableIterator) error {
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

	row, err := data.NextRow()
	for err == nil {
		_, err = fmt.Fprintf(tw, format, row.Cells()...)
		if err != nil {
			return err
		}

		row, err = data.NextRow()
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	return tw.Flush()
}

func printCSV(w io.Writer, data TableIterator) error {
	cw := csv.NewWriter(w)
	cw.Comma = ';'

	header := data.Header()
	lineBuffer := make([]string, len(header))

	line := toStringSlice(header, lineBuffer)
	err := cw.Write(line)
	if err != nil {
		return err
	}

	row, err := data.NextRow()
	for err == nil {
		line = toStringSlice(row.Cells(), lineBuffer)
		err = cw.Write(line)
		if err != nil {
			return err
		}

		row, err = data.NextRow()
	}
	if err != nil && !errors.Is(err, io.EOF) {
		return err
	}

	cw.Flush()
	return cw.Error()
}

func printJSON(w io.Writer, data TableIterator) error {
	if serializable, ok := data.(Serializable); ok {
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		return enc.Encode(serializable.SerializableData())
	}

	_, err := fmt.Fprintln(w, "[")
	if err != nil {
		return err
	}

	var count int
	buf := bytes.NewBuffer(make([]byte, 10240))
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
		serializableRow, ok := row.(Serializable)
		if !ok {
			return errors.New("JSON not supported for sub command")
		}

		buf.Reset()
		err = json.NewEncoder(buf).Encode(serializableRow.SerializableData())
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

	_, err = fmt.Println("\n]")
	return err
}

func toStringSlice(in []any, buf []string) []string {
	for i, v := range in {
		var s string
		switch v := v.(type) {
		case string:
			s = v
		case fmt.Stringer:
			s = v.String()
		default:
			s = fmt.Sprint(v)
		}

		if i < len(buf) {
			buf[i] = s
		} else {
			buf = append(buf, s)
		}
	}
	return buf[0:len(in)]
}
