package datasource

import (
	"context"
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
func (ds *CSVDatasource) createAllSchemaMap() (map[string]*Schema, error) {
	schemaMap := make(map[string]*Schema)

	schema := &Schema{
		Name: ds.URI,
	}
	values, err := readCSVFromURI(ds.URI)
	if err != nil {
		return nil, err
	}
	schema.Columns = make([]*Column, len(values))
	for i := range values {
		if i == ds.ColumnRowIndex {
			for j := range values[i] {
				schema.Columns[i] = &Column{
					Name: values[i][j],
					Type: ColumnType_String,
				}
			}
		}
	}
	schemaMap[schema.Name] = schema
	return schemaMap, nil
}

func (ds *CSVDatasource) GetAllSchema(ctx context.Context) ([]*Schema, error) {
	allMap, err := ds.createAllSchemaMap()
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
func (ds *CSVDatasource) GetSchema(ctx context.Context, name string) (*Schema, error) {
	schemas, err := ds.createAllSchemaMap()
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
func (ds *CSVDatasource) SetSchema(ctx context.Context, schema *Schema) error {
	rows, err := ds.GetRows(ctx, schema)
	if err != nil {
		return err
	}
	return ds.SetRows(ctx, schema, rows)
}

// GetRows is get rows method
func (ds *CSVDatasource) GetRows(ctx context.Context, schema *Schema) ([]*Row, error) {
	csvRows, err := readCSVFromURI(ds.URI)
	if err != nil {
		return nil, err
	}

	rows := make([]*Row, len(csvRows)-1)
	for i, csvRow := range csvRows {
		if i == ds.ColumnRowIndex {
			continue
		}

		rowValues := make(RowValues)
		for k, cn := range schema.GetColumnNames() {
			rowValues[cn] = newStringValue(csvRow[k])
		}
		rows = append(rows, &Row{rowValues})
	}
	return rows, nil
}

// SetRows is set rows method
func (ds *CSVDatasource) SetRows(ctx context.Context, schema *Schema, rows []*Row) error {
	var csvRows [][]string
	for i, row := range rows {
		if i == ds.ColumnRowIndex {
			csvRows = append(csvRows, schema.GetColumnNames())
			continue
		}
		csvRow := make([]string, len(row.values))
		for k, cn := range schema.GetColumnNames() {
			csvRow[k] = row.values[cn].StringValue()
		}
		csvRows = append(csvRows, csvRow)
	}
	return writeCSV(ds.URI, csvRows)
}

func readCSVFromURI(uri string) ([][]string, error) {
	r, err := os.Open(uri)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return readCSV(csv.NewReader(r))
}

func readCSV(r *csv.Reader) ([][]string, error) {
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
