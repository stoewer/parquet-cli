package output

import "errors"

// Format describes a printable data representation.
type Format string

const (
	FormatJSON = "json"
	FormatCSV  = "csv"
	FormatTab  = "tab"
	FormatText = "text"
)

func (f *Format) Validate() error {
	switch *f {
	case FormatJSON, FormatTab, FormatCSV, FormatText:
		return nil
	default:
		return errors.New("output format is expected to be 'json', 'tab', 'text' or 'csv'")
	}
}

func formatsFor(data any) []Format {
	var formats []Format
	switch data.(type) {
	case Serializable, SerializableIterator:
		formats = append(formats, FormatJSON)
	case Table, TableIterator:
		formats = append(formats, FormatTab, FormatCSV)
	case Text:
		formats = append(formats, FormatText)
	}
	return formats
}

func supportsFormat(data any, f Format) bool {
	for _, format := range formatsFor(data) {
		if format == f {
			return true
		}
	}
	return false
}
