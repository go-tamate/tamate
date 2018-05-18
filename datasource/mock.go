package datasource

import (
	"errors"
	"fmt"
)

type MockDatasource struct {
	rows *Rows
}

func NewMockDatasource() (*MockDatasource, error) {
	var values [][]string
	for i := 0; i < 100; i++ {
		row := []string{fmt.Sprintf("id%d", i), fmt.Sprintf("name%d", i)}
		values = append(values, row)
	}
	rows := &Rows{
		Values: values,
	}
	return &MockDatasource{
		rows: rows,
	}, nil
}

func (ds *MockDatasource) Open() error {
	return nil
}

func (ds *MockDatasource) Close() error {
	return nil
}

func (ds *MockDatasource) GetSchemas() ([]*Schema, error) {
	return nil, errors.New("GetSchemas() not supported")
}

func (ds *MockDatasource) GetSchema(sc *Schema) error {
	sc.Columns = []*Column{
		{Name: "id", Type: "string"},
		{Name: "name", Type: "string"},
	}
	sc.PrimaryKey = &PrimaryKey{ColumnNames: []string{"id"}}
	return nil
}

func (ds *MockDatasource) SetSchema(sc *Schema) error {
	return errors.New("SetSchema() not supported on MockDatasource")
}

func (ds *MockDatasource) GetRows(sc *Schema) (*Rows, error) {
	return ds.rows, nil
}

func (ds *MockDatasource) SetRows(sc *Schema, rows *Rows) error {
	ds.rows = rows
	return nil
}
