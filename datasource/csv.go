package datasource

import (
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"regexp"
)

var reg = regexp.MustCompile("\\((.+?)\\)")

type CSVDatasource struct {
	RootPath       string `json:"root_path"`
	ColumnRowIndex int    `json:"column_row_index"`
}

func NewCSVDatasource(rootPath string, columnRowIndex int) (*CSVDatasource, error) {
	return &CSVDatasource{
		RootPath:       rootPath,
		ColumnRowIndex: columnRowIndex,
	}, nil
}

func (ds *CSVDatasource) GetSchema(ctx context.Context, schemaName string) (*Schema, error) {
	values, err := readCSVFromFile(ds.RootPath, schemaName)
	if err != nil {
		return nil, err
	}
	schema := &Schema{
		Name: schemaName,
		PrimaryKey: &Key{
			KeyType: KeyTypePrimary,
		},
	}
	for rowIndex := range values {
		if rowIndex != ds.ColumnRowIndex {
			continue
		}
		columns := make([]*Column, len(values[rowIndex]))
		for colIndex := range values[rowIndex] {
			name := values[rowIndex][colIndex]
			if ret := reg.FindStringSubmatch(name); len(ret) == 2 {
				name = ret[1]
			}
			column := &Column{
				Name:            name,
				OrdinalPosition: colIndex,
				Type:            ColumnTypeString,
			}
			// check primarykey
			if values[rowIndex][colIndex] != name {
				schema.PrimaryKey.ColumnNames = append(schema.PrimaryKey.ColumnNames, name)
			}
			columns[colIndex] = column
		}
		schema.Columns = columns
		break
	}
	return schema, nil
}

func (ds *CSVDatasource) GetRows(ctx context.Context, schema *Schema) ([]*Row, error) {
	values, err := readCSVFromFile(ds.RootPath, schema.Name)
	if err != nil {
		return nil, err
	}

	index := 0
	rows := make([]*Row, len(values)-1)
	for rowIndex := range values {
		if rowIndex == ds.ColumnRowIndex {
			continue
		}
		rowValues := make(RowValues)
		groupByKey := make(GroupByKey)
		for colIndex, col := range schema.Columns {
			columnValue := NewStringGenericColumnValue(col, values[rowIndex][colIndex])
			rowValues[col.Name] = columnValue
			// grouping primarykey
			for i := range schema.PrimaryKey.ColumnNames {
				if schema.PrimaryKey.ColumnNames[i] == col.Name {
					key := schema.PrimaryKey.String()
					groupByKey[key] = append(groupByKey[key], columnValue)
				}
			}
		}
		rows[index] = &Row{GroupByKey: groupByKey, Values: rowValues}
		index++
	}
	return rows, nil
}

func (ds *CSVDatasource) SetSchema(ctx context.Context, schema *Schema) error {
	rows, err := ds.GetRows(ctx, schema)
	if err != nil {
		return err
	}
	return ds.SetRows(ctx, schema, rows)
}

func (ds *CSVDatasource) SetRows(ctx context.Context, schema *Schema, rows []*Row) error {
	var csvRows [][]string
	for i, row := range rows {
		if i == ds.ColumnRowIndex {
			csvRows = append(csvRows, schema.GetColumnNames())
			continue
		}
		csvRow := make([]string, len(row.Values))
		for k, cn := range schema.GetColumnNames() {
			csvRow[k] = row.Values[cn].StringValue()
		}
		csvRows = append(csvRows, csvRow)
	}
	return writeCSV(ds.RootPath, csvRows)
}

func readCSVFromFile(rootPath string, fileName string) ([][]string, error) {
	r, err := os.Open(fmt.Sprintf("%s/%s.csv", rootPath, fileName))
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
