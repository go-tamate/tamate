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
	sc := &Schema{
		Name: "mock",
		PrimaryKey: &Key{
			TableName:   "mock",
			KeyType:     KeyTypePrimary,
			ColumnNames: []string{"id", "name"},
		},
	}
	sc.Columns = []*Column{
		{Name: "id", Type: ColumnTypeString},
		{Name: "name", Type: ColumnTypeString},
	}
	sc.PrimaryKey = &Key{ColumnNames: []string{"id"}}
	return sc, nil
}

func (ds *MockDatasource) SetSchema(ctx context.Context, sc *Schema) error {
	return errors.New("SetSchema() not supported on MockDatasource")
}

func (ds *MockDatasource) GetRows(ctx context.Context, sc *Schema) ([]*Row, error) {
	var rows []*Row
	for i := 0; i < 100; i++ {
		values := make(map[string]*GenericColumnValue)
		groupBykey := make(map[*Key][]*GenericColumnValue)
		for _, col := range sc.Columns {
			values[col.Name] = NewStringGenericColumnValue(col, fmt.Sprintf("%s%d", col.Name, i))
			for _, name := range sc.PrimaryKey.ColumnNames {
				if name == col.Name {
					groupBykey[sc.PrimaryKey] = append(groupBykey[sc.PrimaryKey], values[col.Name])
				}
			}
		}
		rows = append(rows, &Row{GroupByKey: groupBykey, Values: values})
	}
	return rows, nil
}

func (ds *MockDatasource) SetRows(ctx context.Context, sc *Schema, rows []*Row) error {
	return errors.New("MockDatasource does not support SetRows")
}
