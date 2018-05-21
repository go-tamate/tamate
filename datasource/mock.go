package datasource

import (
	"context"
	"errors"
	"fmt"
)

type MockDatasource struct{}

func NewMockDatasource() (*MockDatasource, error) {
	return &MockDatasource{}, nil
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
		{Name: "id", Type: ColumnType_String},
		{Name: "name", Type: ColumnType_String},
	}
	sc.PrimaryKey = &PrimaryKey{ColumnNames: []string{"id"}}
	return sc, nil
}

func (ds *MockDatasource) SetSchema(ctx context.Context, sc *Schema) error {
	return errors.New("SetSchema() not supported on MockDatasource")
}

func (ds *MockDatasource) GetRows(ctx context.Context, sc *Schema) ([]*Row, error) {
	var rows []*Row
	for i := 0; i < 100; i++ {
		values := make(map[string]*GenericColumnValue)
		values["id"] = newStringValue(fmt.Sprintf("id%d", i))
		values["name"] = newStringValue(fmt.Sprintf("name%d", i))
		rows = append(rows, &Row{values: values})
	}
	return rows, nil
}

func (ds *MockDatasource) SetRows(ctx context.Context, sc *Schema, rows []*Row) error {
	return errors.New("MockDatasource does not support SetRows")
}
