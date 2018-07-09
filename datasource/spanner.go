package datasource

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/spanner"
	"google.golang.org/api/iterator"
	sppb "google.golang.org/genproto/googleapis/spanner/v1"
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

		column, err := ScanSchemaColumn(row)
		if err != nil {
			return nil, err
		}
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

func (ds *SpannerDatasource) getPrimaryKey(ctx context.Context, tableName string) (*Key, error) {
	stmt := spanner.NewStatement(fmt.Sprintf("SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.INDEX_COLUMNS WHERE TABLE_NAME = '%s' AND INDEX_TYPE = 'PRIMARY_KEY' ORDER BY ORDINAL_POSITION ASC", tableName))
	iter := ds.spannerClient.Single().Query(ctx, stmt)
	defer iter.Stop()

	var pk *Key
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, err
		}

		if pk == nil {
			pk = &Key{}
		}
		var colName string
		if err := row.ColumnByName("COLUMN_NAME", &colName); err != nil {
			return nil, err
		}
		pk.KeyType = KeyTypePrimary
		pk.ColumnNames = append(pk.ColumnNames, colName)
	}
	return pk, nil
}

func ScanSchemaColumn(row *spanner.Row) (*Column, error) {
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
		return ColumnTypeInt, nil
	}
	if st == "FLOAT64" {
		return ColumnTypeFloat, nil
	}
	if st == "TIMESTAMP" {
		return ColumnTypeDatetime, nil
	}
	if st == "DATE" {
		return ColumnTypeDate, nil
	}
	if st == "BOOL" {
		return ColumnTypeBool, nil
	}
	if strings.HasPrefix(st, "STRING") {
		return ColumnTypeString, nil
	}
	if strings.HasPrefix(st, "BYTES") {
		return ColumnTypeBytes, nil
	}

	// This is a little suck, but for now it's just enough.
	if strings.HasPrefix(st, "ARRAY<STRING") {
		return ColumnTypeStringArray, nil
	}
	if strings.HasPrefix(st, "ARRAY<BYTES") {
		return ColumnTypeBytesArray, nil
	}
	if strings.HasPrefix(st, "ARRAY<DATE") {
		return ColumnTypeDateArray, nil
	}
	if strings.HasPrefix(st, "ARRAY<FLOAT64") {
		return ColumnTypeFloatArray, nil
	}
	if strings.HasPrefix(st, "ARRAY<INT64") {
		return ColumnTypeIntArray, nil
	}
	if strings.HasPrefix(st, "ARRAY<TIMESTAMP") {
		return ColumnTypeDatetimeArray, nil
	}
	if strings.HasPrefix(st, "ARRAY<BOOL") {
		return ColumnTypeBoolArray, nil
	}

	return ColumnTypeNull, fmt.Errorf("cannot convert spanner type: %s", st)
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
		rowValuesGroupByKey := make(GroupByKey)
		for _, c := range schema.Columns {
			var gval spanner.GenericColumnValue
			if err := row.ColumnByName(c.Name, &gval); err != nil {
				return nil, err
			}
			cv, err := GenericSpannerValueToTamateGenericColumnValue(gval, c)
			if err != nil {
				return nil, err
			}
			rowValues[c.Name] = cv
			for _, name := range schema.PrimaryKey.ColumnNames {
				if name == c.Name {
					rowValuesGroupByKey[schema.PrimaryKey.String()] = append(rowValuesGroupByKey[schema.PrimaryKey.String()], cv)
				}
			}
		}
		rows = append(rows, &Row{rowValuesGroupByKey, rowValues})
	}
	return rows, nil
}

func GenericSpannerValueToTamateGenericColumnValue(sp spanner.GenericColumnValue, col *Column) (*GenericColumnValue, error) {
	cv := &GenericColumnValue{Column: col}
	switch sp.Type.GetCode() {
	case sppb.TypeCode_STRING:
		var s spanner.NullString
		if err := sp.Decode(&s); err != nil {
			return nil, err
		}
		if s.Valid {
			cv.Value = s.StringVal
		} else {
			cv.Value = nil
		}
		return cv, nil
	case sppb.TypeCode_INT64:
		var s spanner.NullInt64
		if err := sp.Decode(&s); err != nil {
			return nil, err
		}
		if s.Valid {
			cv.Value = s.Int64
		} else {
			cv.Value = nil
		}
		return cv, nil
	case sppb.TypeCode_FLOAT64:
		var s spanner.NullFloat64
		if err := sp.Decode(&s); err != nil {
			return nil, err
		}
		if s.Valid {
			cv.Value = s.Float64
		} else {
			cv.Value = nil
		}
		return cv, nil
	case sppb.TypeCode_BOOL:
		var s spanner.NullBool
		if err := sp.Decode(&s); err != nil {
			return nil, err
		}
		if s.Valid {
			cv.Value = s.Bool
		} else {
			cv.Value = nil
		}
		return cv, nil
	case sppb.TypeCode_BYTES:
		var s []byte
		if err := sp.Decode(&s); err != nil {
			return nil, err
		}
		cv.Value = s
		return cv, nil
	case sppb.TypeCode_DATE:
		var s spanner.NullDate
		if err := sp.Decode(&s); err != nil {
			return nil, err
		}
		if s.Valid {
			cv.Value = s.Date.String()
		} else {
			cv.Value = nil
		}
		return cv, nil
	case sppb.TypeCode_TIMESTAMP:
		var s spanner.NullTime
		if err := sp.Decode(&s); err != nil {
			return nil, err
		}
		if s.Valid {
			cv.Value = s.Time
		} else {
			cv.Value = nil
		}
		return cv, nil
	case sppb.TypeCode_ARRAY:
		// handle nil
		if li := sp.Value.GetListValue(); li == nil {
			cv.Value = nil
			return cv, nil
		}
		return spannerArrayToTamateGenericColumnValue(sp, col)
	case sppb.TypeCode_STRUCT:
		var s spanner.NullRow
		if err := sp.Decode(&s); err != nil {
			return nil, err
		}
		if s.Valid {
			cv.Value = s.Row
		} else {
			cv.Value = nil
		}
		return cv, nil
	}
	// TODO: additional represents for various spanner types
	return &GenericColumnValue{Column: col, Value: sp.Value.GetStringValue()}, nil
}

func spannerArrayToTamateGenericColumnValue(sp spanner.GenericColumnValue, col *Column) (*GenericColumnValue, error) {
	cv := &GenericColumnValue{Column: col}
	li := make([]interface{}, 0)
	switch sp.Type.ArrayElementType.GetCode() {
	case sppb.TypeCode_STRING:
		var s []spanner.NullString
		if err := sp.Decode(&s); err != nil {
			return nil, err
		}
		for i := 0; i < len(s); i++ {
			if s[i].Valid {
				li = append(li, s[i].StringVal)
			} else {
				li = append(li, nil)
			}
		}
		cv.Value = li
		return cv, nil
	case sppb.TypeCode_INT64:
		var s []spanner.NullInt64
		if err := sp.Decode(&s); err != nil {
			return nil, err
		}
		for i := 0; i < len(s); i++ {
			if s[i].Valid {
				li = append(li, s[i].Int64)
			} else {
				li = append(li, nil)
			}
		}
		cv.Value = li
		return cv, nil
	case sppb.TypeCode_FLOAT64:
		var s []spanner.NullFloat64
		if err := sp.Decode(&s); err != nil {
			return nil, err
		}
		for i := 0; i < len(s); i++ {
			if s[i].Valid {
				li = append(li, s[i].Float64)
			} else {
				li = append(li, nil)
			}
		}
		cv.Value = li
		return cv, nil
	case sppb.TypeCode_BOOL:
		var s []spanner.NullBool
		if err := sp.Decode(&s); err != nil {
			return nil, err
		}
		for i := 0; i < len(s); i++ {
			if s[i].Valid {
				li = append(li, s[i].Bool)
			} else {
				li = append(li, nil)
			}
		}
		cv.Value = li
		return cv, nil
	case sppb.TypeCode_BYTES:
		var s [][]byte
		if err := sp.Decode(&s); err != nil {
			return nil, err
		}
		cv.Value = s
		return cv, nil
	case sppb.TypeCode_DATE:
		var s []spanner.NullDate
		if err := sp.Decode(&s); err != nil {
			return nil, err
		}
		for i := 0; i < len(s); i++ {
			if s[i].Valid {
				li = append(li, s[i].Date.String())
			} else {
				li = append(li, nil)
			}
		}
		cv.Value = li
		return cv, nil
	case sppb.TypeCode_TIMESTAMP:
		var s []spanner.NullTime
		if err := sp.Decode(&s); err != nil {
			return nil, err
		}
		for i := 0; i < len(s); i++ {
			if s[i].Valid {
				li = append(li, s[i].Time)
			} else {
				li = append(li, nil)
			}
		}
		cv.Value = li
		return cv, nil
	case sppb.TypeCode_STRUCT:
		var s []spanner.NullRow
		if err := sp.Decode(&s); err != nil {
			return nil, err
		}
		for i := 0; i < len(s); i++ {
			if s[i].Valid {
				li = append(li, s[i].Row)
			} else {
				li = append(li, nil)
			}
		}
		cv.Value = li
		return cv, nil
	default:
		return nil, errors.New("No spanner type matched")
	}
}

// SetRows is set rows method
func (ds *SpannerDatasource) SetRows(ctx context.Context, schema *Schema, rows []*Row) error {
	return errors.New("SpannerDatasource does not support SetRows()")
}

// ConvertGenericColumnValueToSpannerValue converts GenericColumnValue to Spanner Value
func ConvertGenericColumnValueToSpannerValue(cv *GenericColumnValue) (interface{}, error) {
	if cv.Column.NotNull && cv.Value == nil {
		return nil, errors.New("this value must not be null")
	}
	switch cv.Column.Type {
	case ColumnTypeString:
		if !cv.Column.NotNull && cv.Value == nil {
			return spanner.NullString{}, nil
		}
		return cv.StringValue(), nil
	case ColumnTypeInt:
		if !cv.Column.NotNull && cv.Value == nil {
			return spanner.NullInt64{}, nil
		}
		// Why this type assertion see int as float64?
		switch cv.Value.(type) {
		case float64:
			f, err := strconv.ParseFloat(cv.StringValue(), 64)
			if err != nil {
				return nil, err
			}
			return int64(f), nil
		case int64:
			i, err := strconv.ParseInt(cv.StringValue(), 10, 64)
			if err != nil {
				return nil, err
			}
			return i, nil
		default:
			i, err := strconv.ParseInt(cv.StringValue(), 10, 64)
			if err != nil {
				return nil, err
			}
			return i, nil
		}
	case ColumnTypeFloat:
		if !cv.Column.NotNull && cv.Value == nil {
			return spanner.NullFloat64{}, nil
		}
		f, err := strconv.ParseFloat(cv.StringValue(), 64)
		if err != nil {
			return nil, err
		}
		return f, nil
	case ColumnTypeDatetime:
		if !cv.Column.NotNull && cv.Value == nil {
			return spanner.NullTime{}, nil
		}
		return cv.Value, nil
	case ColumnTypeDate:
		if !cv.Column.NotNull && cv.Value == nil {
			return spanner.NullDate{}, nil
		}
		return cv.Value, nil
	case ColumnTypeBytes:
		if !cv.Column.NotNull && cv.Value == nil {
			return spanner.NullString{}, nil
		}
		return cv.Value, nil
	case ColumnTypeBool:
		if !cv.Column.NotNull && cv.Value == nil {
			return spanner.NullBool{}, nil
		}
		return cv.Value, nil
	case ColumnTypeFloatArray:
		if !cv.Column.NotNull && cv.Value == nil {
			return nil, nil
		}
		values := make([]float64, 0)
		if arr, ok := cv.Value.([]interface{}); ok {
			for _, v := range arr {
				if f, ok := v.(float64); ok {
					values = append(values, f)
				}
			}
			if len(arr) != len(values) {
				return nil, errors.New("length mismatch, some value failed to convert into float64")
			}
			return values, nil
		}
		return nil, fmt.Errorf("failed to convert %v as array", cv.Value)
	case ColumnTypeIntArray:
		if !cv.Column.NotNull && cv.Value == nil {
			return nil, nil
		}
		// Why this type assertion see int as float64?
		values := make([]int64, 0)
		if arr, ok := cv.Value.([]interface{}); ok {
			for _, v := range arr {
				if i, ok := v.(int64); ok {
					values = append(values, i)
				} else if i, ok := v.(float64); ok {
					values = append(values, int64(i))
				}
			}
			if len(arr) != len(values) {
				return nil, errors.New("length mismatch, some value failed to convert into int64")
			}
			return values, nil
		}
		return nil, fmt.Errorf("failed to convert %v as array", cv.Value)
	case ColumnTypeDateArray:
		if !cv.Column.NotNull && cv.Value == nil {
			return nil, nil
		}
		values := make([]string, 0)
		if arr, ok := cv.Value.([]interface{}); ok {
			for _, v := range arr {
				if s, ok := v.(string); ok {
					if _, err := time.Parse("2006-01-02", s); err != nil {
						return nil, errors.New("failed to parse string to Date format yyyy-mm-dd")
					}
					values = append(values, s)
				}
			}
			if len(arr) != len(values) {
				return nil, errors.New("length mismatch, some value failed to convert into date")
			}
			return values, nil
		}
		return nil, fmt.Errorf("failed to convert %v as array", cv.Value)
	case ColumnTypeDatetimeArray:
		if !cv.Column.NotNull && cv.Value == nil {
			return nil, nil
		}
		values := make([]time.Time, 0)
		if arr, ok := cv.Value.([]interface{}); ok {
			for _, v := range arr {
				if s, ok := v.(string); ok {
					t, err := time.Parse(time.RFC3339Nano, s)
					if err != nil {
						return nil, errors.New("failed to parse string to date format yyyy-mm-dd")
					}
					values = append(values, t)
				}
			}
			if len(arr) != len(values) {
				return nil, errors.New("length mismatch, some value failed to convert into string")
			}
			return values, nil
		}
		return nil, fmt.Errorf("failed to convert %v as array", cv.Value)
	case ColumnTypeStringArray:
		if !cv.Column.NotNull && cv.Value == nil {
			return nil, nil
		}
		values := make([]string, 0)
		if arr, ok := cv.Value.([]interface{}); ok {
			for _, v := range arr {
				if s, ok := v.(string); ok {
					values = append(values, s)
				}
			}
			if len(arr) != len(values) {
				return nil, errors.New("length mismatch, some value failed to convert into string")
			}
			return values, nil
		}
		return nil, fmt.Errorf("failed to convert %v as array", cv.Value)
	case ColumnTypeBytesArray:
		if !cv.Column.NotNull && cv.Value == nil {
			return nil, nil
		}

		// type assertion to string is ok but, if we save as []byte,
		// value will change after insert into Spanner...
		// so, if assertion success on string, convert them to []string
		var values [][]byte
		var strValues []string
		if arr, ok := cv.Value.([]interface{}); ok {
			for _, v := range arr {
				if b, ok := v.([]byte); ok {
					values = append(values, b)
				} else if b, ok := v.(string); ok {
					strValues = append(strValues, b)
				}
			}
			if len(arr) != len(values) && len(arr) != len(strValues) {
				return nil, errors.New("length mismatch, some value failed to convert into []byte")
			}
			if len(arr) != len(values) {
				return strValues, nil
			}
			return values, nil
		}
		return nil, fmt.Errorf("failed to convert %v as array", cv.Value)
	case ColumnTypeBoolArray:
		if !cv.Column.NotNull && cv.Value == nil {
			return nil, nil
		}
		values := make([]bool, 0)
		if arr, ok := cv.Value.([]interface{}); ok {
			for _, v := range arr {
				if b, ok := v.(bool); ok {
					values = append(values, b)
				}
			}
			if len(arr) != len(values) {
				return nil, errors.New("length mismatch, some value failed to convert into bool")
			}
			return values, nil
		}
		// TODO: support struct array
		return nil, fmt.Errorf("failed to convert %v as array", cv.Value)
	case ColumnTypeNull:
		fallthrough
	default:
		return nil, nil
	}
}
