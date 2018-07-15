package datasource

import (
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"regexp"
)

// CSVDatasource is datasource config for csv file
type CSVDatasource struct {
	RootPath       string `json:"root_path"`
	ColumnRowIndex int    `json:"column_row_index"`
}

// NewCSVDatasource is create CSVDatasource instance
func NewCSVDatasource(rootPath string, columnRowIndex int) (*CSVDatasource, error) {
	if columnRowIndex < 0 {
		return nil, fmt.Errorf("columnRowIndex is invalid value: %d", columnRowIndex)
	}
	return &CSVDatasource{
		RootPath:       rootPath,
		ColumnRowIndex: columnRowIndex,
	}, nil
}

// GetSchema is getting schema from csv file
func (ds *CSVDatasource) GetSchema(ctx context.Context, name string) (*Schema, error) {
	values, err := readFromFile(ds.RootPath, name)
	if err != nil {
		return nil, err
	}
	primaryKey := &Key{
		KeyType: KeyTypePrimary,
	}
	cols := make([]*Column, 0)
	for rowIndex, row := range values {
		if rowIndex != ds.ColumnRowIndex {
			continue
		}
		for colIndex := range row {
			colName := row[colIndex]
			// check primarykey
			reg := regexp.MustCompile("\\((.+?)\\)")
			if ret := reg.FindStringSubmatch(colName); len(ret) >= 2 {
				colName = ret[1]
				primaryKey.ColumnNames = append(primaryKey.ColumnNames, colName)
			}
			cols = append(cols, &Column{
				Name:            colName,
				OrdinalPosition: colIndex,
				Type:            ColumnTypeString,
			})
		}
		break
	}
	return &Schema{
		Name:       name,
		PrimaryKey: primaryKey,
		Columns:    cols,
	}, nil
}

// GetRows is getting rows from csv file
func (ds *CSVDatasource) GetRows(ctx context.Context, schema *Schema) ([]*Row, error) {
	values, err := readFromFile(ds.RootPath, schema.Name)
	if err != nil {
		return nil, err
	}
	if len(values) > ds.ColumnRowIndex {
		valuesWithoutColumn := make([][]string, len(values)-1)
		for rowIndex, row := range values {
			if rowIndex < ds.ColumnRowIndex {
				valuesWithoutColumn[rowIndex] = row
			} else if rowIndex > ds.ColumnRowIndex {
				valuesWithoutColumn[rowIndex-1] = row
			}
		}
		values = valuesWithoutColumn
	}
	rows := make([]*Row, len(values))
	for rowIndex, row := range values {
		rowValues := make(RowValues, len(schema.Columns))
		groupByKey := make(GroupByKey)
		for colIndex, col := range schema.Columns {
			colValue := NewStringGenericColumnValue(col, row[colIndex])
			rowValues[col.Name] = colValue
			// grouping primarykey
			for i := range schema.PrimaryKey.ColumnNames {
				if schema.PrimaryKey.ColumnNames[i] == col.Name {
					key := schema.PrimaryKey.String()
					groupByKey[key] = append(groupByKey[key], colValue)
				}
			}
		}
		rows[rowIndex] = &Row{GroupByKey: groupByKey, Values: rowValues}
	}
	return rows, nil
}

// SetSchema is setting schema to csv file
func (ds *CSVDatasource) SetSchema(ctx context.Context, schema *Schema) error {
	return fmt.Errorf("feature support")
}

// SetRows is setting rows to csv file
func (ds *CSVDatasource) SetRows(ctx context.Context, schema *Schema, rows []*Row) error {
	return fmt.Errorf("feature support")
}

func readFromFile(rootPath string, fileName string) ([][]string, error) {
	filePath := fmt.Sprintf("%s/%s.csv", rootPath, fileName)
	r, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return read(r)
}

func read(r io.Reader) ([][]string, error) {
	values, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, err
	}
	return values, err
}

func writeToFile(rootPath string, fileName string, values [][]string) error {
	filePath := fmt.Sprintf("%s/%s.csv", rootPath, fileName)
	w, err := os.OpenFile(filePath, os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		return err
	}
	defer w.Close()
	return write(w, values)
}

func write(w io.Writer, values [][]string) error {
	return csv.NewWriter(w).WriteAll(values)
}
