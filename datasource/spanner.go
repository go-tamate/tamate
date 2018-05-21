package datasource

import (
	"context"
	"errors"
	"fmt"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	sppb "google.golang.org/genproto/googleapis/spanner/v1"
	"strings"
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

func (ds *SpannerDatasource) Close() error {
	if ds.spannerClient != nil {
		ds.spannerClient.Close()
	}
	return nil
}

func (ds *SpannerDatasource) createAllSchemaMap(ctx context.Context) (map[string]*Schema, error) {
	stmt := spanner.NewStatement("SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, SPANNER_TYPE, IS_NULLABLE FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = ''")
	iter := ds.spannerClient.Single().Query(ctx, stmt)
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
		pk, err := ds.getPrimaryKey(ctx, tableName)
		if err != nil {
			return nil, err
		}
		schema.PrimaryKey = pk
	}
	return schemaMap, nil
}

func (ds *SpannerDatasource) GetAllSchema(ctx context.Context) ([]*Schema, error) {
	allMap, err := ds.createAllSchemaMap(ctx)
	if err != nil {
		return nil, err
	}

	var all []*Schema
	for _, sc := range allMap {
		all = append(all, sc)
	}
	return all, nil
}

func (ds *SpannerDatasource) getPrimaryKey(ctx context.Context, tableName string) (*PrimaryKey, error) {
	stmt := spanner.NewStatement(fmt.Sprintf("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.INDEX_COLUMNS WHERE TABLE_NAME = '%s' AND INDEX_TYPE = 'PRIMARY_KEY' ORDER BY ORDINAL_POSITION ASC", tableName))
	iter := ds.spannerClient.Single().Query(ctx, stmt)
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
	ct, err := spannerTypeNameToColumnType(columnType)
	if err != nil {
		return nil, err
	}
	return &Column{
		Name:            columnName,
		OrdinalPosition: int(ordinalPosition),
		Type:            ct,
		NotNull:         isNullable == "NO",
		AutoIncrement:   false, // Cloud Spanner does not support AUTO_INCREMENT
	}, nil
}

func spannerTypeNameToColumnType(st string) (ColumnType, error) {
	if st == "INT64" {
		return ColumnType_Int, nil
	}
	if st == "FLOAT64" {
		return ColumnType_Float, nil
	}
	if strings.HasPrefix(st, "STRING") {
		return ColumnType_String, nil
	}
	return ColumnType_Null, fmt.Errorf("cannot convert spanner type: %s", st)
}

// GetSchema is get schema method
func (ds *SpannerDatasource) GetSchema(ctx context.Context, name string) (*Schema, error) {
	all, err := ds.createAllSchemaMap(ctx)
	if err != nil {
		return nil, err
	}

	for scName, sc := range all {
		if scName == name {
			return sc, nil
		}
	}
	return nil, errors.New("Schema not found: " + name)

}

// SetSchema is set schema method
func (ds *SpannerDatasource) SetSchema(ctx context.Context, schema *Schema) error {
	return errors.New("not implemented")
}

// GetRows is get rows method
func (ds *SpannerDatasource) GetRows(ctx context.Context, schema *Schema) ([]*Row, error) {
	stmt := spanner.NewStatement(fmt.Sprintf("SELECT * FROM `%s`", schema.Name))
	iter := ds.spannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	var rows []*Row
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		rowValues := make(RowValues)
		for _, cn := range row.ColumnNames() {
			var gval spanner.GenericColumnValue
			if err := row.ColumnByName(cn, &gval); err != nil {
				return nil, err
			}
			cv, err := genericSpannerValueToTamateGenericColumnValue(gval)
			if err != nil {
				return nil, err
			}
			rowValues[cn] = cv
		}
		rows = append(rows, &Row{rowValues})
	}
	return rows, nil
}

func genericSpannerValueToTamateGenericColumnValue(sp spanner.GenericColumnValue) (*GenericColumnValue, error) {
	if sp.Type.GetCode() == sppb.TypeCode_FLOAT64 {
		return newFloatValue(sp.Value.GetNumberValue()), nil
	}
	// TODO: additional represents for various spanner types
	return newStringValue(sp.Value.GetStringValue()), nil
}

// SetRows is set rows method
func (ds *SpannerDatasource) SetRows(ctx context.Context, schema *Schema, rows []*Row) error {
	return errors.New("SpannerDatasource does not support SetRows()")
}
