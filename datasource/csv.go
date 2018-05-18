package datasource

import (
	"encoding/csv"
	"errors"
	"os"
)

// CSVDatasource is datasource struct of csv
type CSVDatasource struct {
	URI            string `json:"uri"`
	ColumnRowIndex int    `json:"column_row_index"`
}

// NewCSVDatasource is create CSVDatasource instance method
func NewCSVDatasource(uri string, columnRowIndex int) (*CSVDatasource, error) {
	return &CSVDatasource{
		URI:            uri,
		ColumnRowIndex: columnRowIndex,
	}, nil
}

// GetSchemas is get all schemas method
func (h *CSVDatasource) createAllSchemaMap() (map[string]*Schema, error) {
	schemaMap := make(map[string]*Schema)

	schema := &Schema{
		Name: h.URI,
	}
	values, err := readCSV(h.URI)
	if err != nil {
		return nil, err
	}
	schema.Columns = make([]*Column, len(values))
	for i := range values {
		if i == h.ColumnRowIndex {
			for j := range values[i] {
				schema.Columns[i] = &Column{
					Name: values[i][j],
					Type: "string",
				}
			}
		}
	}
	schemaMap[schema.Name] = schema
	return schemaMap, nil
}

func (h *CSVDatasource) GetAllSchema() ([]*Schema, error) {
	allMap, err := h.createAllSchemaMap()
	if err != nil {
		return nil, err
	}

	var all []*Schema
	for _, sc := range allMap {
		all = append(all, sc)
	}
	return all, nil
}

// GetSchema is get schema method
func (h *CSVDatasource) GetSchema(name string) (*Schema, error) {
	schemas, err := h.createAllSchemaMap()
	if err != nil {
		return nil, err
	}
	for _, sc := range schemas {
		if sc.Name == name {
			return sc, nil
		}
	}
	return nil, errors.New("Schema not found: " + name)
}

// SetSchema is set schema method
func (h *CSVDatasource) SetSchema(schema *Schema) error {
	rows, err := h.GetRows(schema)
	if err != nil {
		return err
	}
	values := make([][]string, 0)
	for i := range rows.Values {
		if i == h.ColumnRowIndex {
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
func (h *CSVDatasource) GetRows(schema *Schema) (*Rows, error) {
	values, err := readCSV(h.URI)
	if err != nil {
		return nil, err
	}
	// drop column row
	_values := make([][]string, 0)
	for i, value := range values {
		if i == h.ColumnRowIndex {
			continue
		}
		_values = append(_values, value)
	}
	values = _values
	return &Rows{
		Values: values,
	}, nil
}

// SetRows is set rows method
func (h *CSVDatasource) SetRows(schema *Schema, rows *Rows) error {
	var values [][]string
	for j := range rows.Values {
		if j == h.ColumnRowIndex {
			_, err := h.GetSchema(schema.Name)
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
