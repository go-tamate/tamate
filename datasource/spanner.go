package datasource

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
)

type SpannerDatasource struct {
	DSN           string `json:"dsn"`
	spannerClient *spanner.Client
}

func NewSpannerDatasource(dsn string) (*SpannerDatasource, error) {
	ctx := context.Background()
	spannerClient, err := spanner.NewClient(ctx, dsn)
	if err != nil {
		return nil, err
	}
	return &SpannerDatasource{
		DSN:           dsn,
		spannerClient: spannerClient,
	}, nil
}

func (h *SpannerDatasource) Open() error {
	return nil
}

func (h *SpannerDatasource) Close() error {
	return nil
}

func (h *SpannerDatasource) GetSchemas() ([]*Schema, error) {
	ctx := context.Background()
	stmt := spanner.NewStatement("SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, SPANNER_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ''")
	iter := h.spannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	// scan results
	schemaMap := make(map[string]*Schema)
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		var tableName string
		if err := row.ColumnByName("TABLE_NAME", &tableName); err != nil {
			return nil, err
		}

		column, err := scanSchemaColumn(row)
		if _, ok := schemaMap[tableName]; !ok {
			schemaMap[tableName] = &Schema{Name: tableName}
		}
		schemaMap[tableName].Columns = append(schemaMap[tableName].Columns, column)
	}

	for tableName, schema := range schemaMap {
		pk, err := h.getPrimaryKey(tableName)
		if err != nil {
			return nil, err
		}
		schema.PrimaryKey = pk
	}

	// set schemas
	var schemas []*Schema
	for tableName := range schemaMap {
		schemas = append(schemas, schemaMap[tableName])
	}
	return schemas, nil
}

func (h *SpannerDatasource) getPrimaryKey(tableName string) (*PrimaryKey, error) {
	ctx := context.Background()
	stmt := spanner.NewStatement(fmt.Sprintf("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.INDEX_COLUMNS WHERE TABLE_NAME = '%s' AND INDEX_TYPE = 'PRIMARY_KEY' ORDER BY ORDINAL_POSITION ASC", tableName))
	iter := h.spannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	var pk *PrimaryKey
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		if pk == nil {
			pk = &PrimaryKey{}
		}
		var colName string
		if err := row.ColumnByName("COLUMN_NAME", &colName); err != nil {
			return nil, err
		}
		pk.ColumnNames = append(pk.ColumnNames, colName)
	}
	return pk, nil
}

func scanSchemaColumn(row *spanner.Row) (*Column, error) {
	var columnName string
	var tableName string
	var ordinalPosition int64
	var columnType string
	var isNullable string
	if err := row.Columns(&tableName, &columnName, &ordinalPosition, &columnType, &isNullable); err != nil {
		return nil, err
	}
	return &Column{
		Name:            columnName,
		OrdinalPosition: int(ordinalPosition),
		Type:            columnType,
		NotNull:         isNullable == "NO",
		AutoIncrement:   false, // Cloud Spanner does not support AUTO_INCREMENT
	}, nil
}

// GetSchema is get schema method
func (h *SpannerDatasource) GetSchema(name string) (*Schema, error) {
	return nil, errors.New("not implemented")
}

// SetSchema is set schema method
func (h *SpannerDatasource) SetSchema(schema *Schema) error {
	return errors.New("not implemented")
}

// GetRows is get rows method
func (h *SpannerDatasource) GetRows(schema *Schema) (*Rows, error) {
	ctx := context.Background()
	stmt := spanner.NewStatement(fmt.Sprintf("SELECT * FROM `%s`", schema.Name))
	iter := h.spannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	var values [][]string
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		value := make([]string, row.Size())
		for n := 0; n < row.Size(); n++ {
			row.Column(n, &value[n])
		}
		values = append(values, value)
	}
	return &Rows{
		Values: values,
	}, nil
}

// SetRows is set rows method
func (h *SpannerDatasource) SetRows(schema *Schema, rows *Rows) error {
	ctx := context.Background()
	if _, err := h.spannerClient.ReadWriteTransaction(ctx, func(ctx context.Context, tx *spanner.ReadWriteTransaction) error {
		var ms []*spanner.Mutation
		for _, value := range rows.Values {
			insertRow := make([]interface{}, len(value))
			for i, v := range value {
				insertRow[i] = v
			}
			ms = append(ms, spanner.InsertOrUpdate(schema.Name, schema.GetColumnNames(), insertRow))
		}
		return tx.BufferWrite(ms)
	}); err != nil {
		return err
	}
	return nil
}
