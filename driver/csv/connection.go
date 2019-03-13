package csv

import (
	"context"
	"fmt"
	"regexp"

	"github.com/Mitu217/tamate/driver"
)

type csvConn struct {
	rootPath       string
	columnRowIndex int
}

func (c *csvConn) Close() error {
	return nil
}

func (cc *csvConn) GetSchema(ctx context.Context, fileName string) (*driver.Schema, error) {
	values, err := readFromFile(cc.rootPath, fileName)
	if err != nil {
		return nil, err
	}
	primaryKey := &driver.Key{
		KeyType: driver.KeyTypePrimary,
	}
	cols := make([]*driver.Column, 0)
	for rowIndex, row := range values {
		if rowIndex != cc.columnRowIndex {
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
			cols = append(cols, &driver.Column{
				Name:            colName,
				OrdinalPosition: colIndex,
				Type:            driver.ColumnTypeString,
			})
		}
		break
	}
	return &driver.Schema{
		Name:       fileName,
		PrimaryKey: primaryKey,
		Columns:    cols,
	}, nil
}

func (cc *csvConn) SetSchema(ctx context.Context, schema *driver.Schema) error {
	return fmt.Errorf("feature support")
}

func (cc *csvConn) GetRows(ctx context.Context, schema *driver.Schema) ([]*driver.Row, error) {
	values, err := readFromFile(cc.rootPath, schema.Name)
	if err != nil {
		return nil, err
	}
	if len(values) > cc.columnRowIndex {
		valuesWithoutColumn := make([][]string, len(values)-1)
		for rowIndex, row := range values {
			if rowIndex < cc.columnRowIndex {
				valuesWithoutColumn[rowIndex] = row
			} else if rowIndex > cc.columnRowIndex {
				valuesWithoutColumn[rowIndex-1] = row
			}
		}
		values = valuesWithoutColumn
	}
	rows := make([]*driver.Row, len(values))
	for rowIndex, row := range values {
		rowValues := make(driver.RowValues, len(schema.Columns))
		groupByKey := make(driver.GroupByKey)
		for colIndex, col := range schema.Columns {
			colValue := driver.NewGenericColumnValue(col, row[colIndex])
			rowValues[col.Name] = colValue
			// grouping primarykey
			for i := range schema.PrimaryKey.ColumnNames {
				if schema.PrimaryKey.ColumnNames[i] == col.Name {
					key := schema.PrimaryKey.String()
					groupByKey[key] = append(groupByKey[key], colValue)
				}
			}
		}
		rows[rowIndex] = &driver.Row{GroupByKey: groupByKey, Values: rowValues}
	}
	return rows, nil
}

func (cc *csvConn) SetRows(ctx context.Context, scehma *driver.Schema, rows []*driver.Row) error {
	return fmt.Errorf("feature support")
}
