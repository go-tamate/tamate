package datasource

import (
	"context"
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

func (ds *MockDatasource) GetAllSchema(ctx context.Context) ([]*Schema, error) {
	sc, err := ds.GetSchema(ctx, "")
	if err != nil {
		return nil, err
	}
	return []*Schema{sc}, nil
}

func (ds *MockDatasource) GetSchema(ctx context.Context, name string) (*Schema, error) {
	sc := &Schema{}
	sc.Columns = []*Column{
		{Name: "id", Type: "string"},
		{Name: "name", Type: "string"},
	}
	sc.PrimaryKey = &PrimaryKey{ColumnNames: []string{"id"}}
	return sc, nil
}

func (ds *MockDatasource) SetSchema(ctx context.Context, sc *Schema) error {
	return errors.New("SetSchema() not supported on MockDatasource")
}

func (ds *MockDatasource) GetRows(ctx context.Context, sc *Schema) (*Rows, error) {
	return ds.rows, nil
}

func (ds *MockDatasource) SetRows(ctx context.Context, sc *Schema, rows *Rows) error {
	ds.rows = rows
	return nil
}
