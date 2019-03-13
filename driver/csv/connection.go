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

func (c *csvConn) GetSchema(ctx context.Context, fileName string) (*driver.Schema, error) {
	values, err := readFromFile(c.rootPath, fileName)
	if err != nil {
		return nil, err
	}
	primaryKey := &driver.Key{
		KeyType: driver.KeyTypePrimary,
	}
	cols := make([]*driver.Column, 0)
	for rowIndex, row := range values {
		if rowIndex != c.columnRowIndex {
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

func (c *csvConn) SetSchema(ctx context.Context, name string, schema *driver.Schema) error {
	return fmt.Errorf("feature support")
}

func (c *csvConn) GetRows(ctx context.Context, name string) ([]*driver.Row, error) {
	values, err := readFromFile(c.rootPath, name)
	if err != nil {
		return nil, err
	}
	if len(values) > c.columnRowIndex {
		valuesWithoutColumn := make([][]string, len(values)-1)
		for rowIndex, row := range values {
			if rowIndex < c.columnRowIndex {
				valuesWithoutColumn[rowIndex] = row
			} else if rowIndex > c.columnRowIndex {
				valuesWithoutColumn[rowIndex-1] = row
			}
		}
		values = valuesWithoutColumn
	}
	/*
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
	*/
	return nil, nil
}

func (c *csvConn) SetRows(ctx context.Context, name string, rows []*driver.Row) error {
	return fmt.Errorf("feature support")
}
