package handler

import (
	"encoding/csv"
	"os"
)

// CSVHandler is handler struct of csv
type CSVHandler struct {
	URI            string `json:"uri"`
	ColumnRowIndex int    `json:"column_row_index"`
}

// NewCSVHandler is create CSVHandler instance method
func NewCSVHandler(uri string, columnRowIndex int) (*CSVHandler, error) {
	return &CSVHandler{
		URI:            uri,
		ColumnRowIndex: columnRowIndex,
	}, nil
}

// Open is call by datasource when create instance
func (h *CSVHandler) Open() error {
	return nil
}

// Close is call by datasource when free instance
func (h *CSVHandler) Close() error {
	return nil
}

// GetSchemas is get all schemas method
func (h *CSVHandler) GetSchemas() ([]*Schema, error) {
	schema := &Schema{
		Name: h.URI,
	}
	if h.ColumnRowIndex > 0 {
		values, err := readCSV(h.URI)
		if err != nil {
			return nil, err
		}
		for i := range values {
			if i == h.ColumnRowIndex-1 {
				columns := []Column{}
				for j := range values[i] {
					column := Column{
						Name: values[i][j],
						Type: "string",
					}
					columns = append(columns, column)
				}
				schema.Columns = columns
			}
		}
	}
	return []*Schema{schema}, nil
}

// GetSchema is get schema method
func (h *CSVHandler) GetSchema(schema *Schema) error {
	schemas, err := h.GetSchemas()
	if err != nil {
		return err
	}
	for _, sc := range schemas {
		if sc.Name == schema.Name {
			schema.Columns = sc.Columns
		}
	}
	return nil
}

// SetSchema is set schema method
func (h *CSVHandler) SetSchema(schema *Schema) error {
	rows, err := h.GetRows(schema)
	if err != nil {
		return err
	}
	values := make([][]string, 0)
	for i := range rows.Values {
		if i == h.ColumnRowIndex-1 {
			schemaValue := make([]string, len(schema.Columns))
			for j := range schema.Columns {
				schemaValue[j] = schema.Columns[j].Name
			}
			values = append(values, schemaValue)
		}
		values = append(values, rows.Values[i])
	}
	return writeCSV(h.URI, values)
}

// GetRows is get rows method
func (h *CSVHandler) GetRows(schema *Schema) (*Rows, error) {
	values, err := readCSV(h.URI)
	if err != nil {
		return nil, err
	}
	if h.ColumnRowIndex > 0 {
		// drop column row
		_values := make([][]string, 0)
		for i, value := range values {
			if i == h.ColumnRowIndex-1 {
				continue
			}
			_values = append(_values, value)
		}
		values = _values
	}
	return &Rows{
		Values: values,
	}, nil
}

// SetRows is set rows method
func (h *CSVHandler) SetRows(schema *Schema, rows *Rows) error {
	values := make([][]string, 0)
	for j := range rows.Values {
		if j == h.ColumnRowIndex-1 {
			err := h.GetSchema(schema)
			if err != nil {
				return err
			}
			schemaValue := make([]string, len(schema.Columns))
			for j := range schema.Columns {
				schemaValue[j] = schema.Columns[j].Name
			}
			values = append(values, schemaValue)
		}
		values = append(values, rows.Values[j])
	}
	return writeCSV(h.URI, values)
}

func readCSV(uri string) ([][]string, error) {
	r, err := os.Open(uri)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return read(csv.NewReader(r))
}

func read(r *csv.Reader) ([][]string, error) {
	values, err := r.ReadAll()
	if err != nil {
		return nil, err
	}
	return values, err
}

func writeCSV(uri string, values [][]string) error {
	w, err := os.OpenFile(uri, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer w.Close()
	return write(csv.NewWriter(w), values)
}

func write(w *csv.Writer, values [][]string) error {
	return w.WriteAll(values)
}
