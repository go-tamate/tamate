package datasource

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
)

type MySQLDatasource struct {
	DSN string `json:"dsn"`
	db  *sql.DB
}

func NewMySQLDatasource(dsn string) (*MySQLDatasource, error) {
	return &MySQLDatasource{
		DSN: dsn,
	}, nil
}

func (ds *MySQLDatasource) Open() error {
	if ds.db == nil {
		db, err := sql.Open("mysql", ds.DSN)
		if err != nil {
			return err
		}
		if err := db.Ping(); err != nil {
			return err
		}
		ds.db = db
	}
	return nil
}

func (ds *MySQLDatasource) Close() error {
	if ds.db != nil {
		err := ds.db.Close()
		ds.db = nil
		if err != nil {
			return err
		}
	}
	return nil
}

func (ds *MySQLDatasource) createAllSchemaMap() (map[string]*Schema, error) {
	// get schemas
	sqlRows, err := ds.db.Query("SELECT TABLE_NAME, COLUMN_NAME, ORDINAL_POSITION, COLUMN_TYPE, COLUMN_KEY, IS_NULLABLE, EXTRA FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE()")
	if err != nil {
		return nil, err
	}
	defer sqlRows.Close()

	// scan results
	schemaMap := make(map[string]*Schema)
	for sqlRows.Next() {
		var tableName string
		var columnName string
		var ordinalPosition int
		var columnType string
		var columnKey string
		var isNullable string
		var extra string
		if err := sqlRows.Scan(&tableName, &columnName, &ordinalPosition, &columnType, &columnKey, &isNullable, &extra); err != nil {
			return nil, err
		}
		// prepare schema
		if _, ok := schemaMap[tableName]; !ok {
			schemaMap[tableName] = &Schema{Name: tableName}
		}
		schema := schemaMap[tableName]
		// set column in schema
		if strings.Contains(columnKey, "PRI") {
			if schema.PrimaryKey == nil {
				schema.PrimaryKey = &Key{
					TableName: schema.Name,
					KeyType:   KeyTypePrimary,
				}
			}
			schema.PrimaryKey.ColumnNames = append(schema.PrimaryKey.ColumnNames, columnName)
		}
		valueType, err := mysqlColumnTypeToValueType(columnType)
		if err != nil {
			return nil, err
		}
		column := &Column{
			Name:            columnName,
			OrdinalPosition: ordinalPosition - 1,
			Type:            valueType,
			NotNull:         isNullable != "YES",
			AutoIncrement:   strings.Contains(extra, "auto_increment"),
		}
		schema.Columns = append(schema.Columns, column)
		schemaMap[tableName] = schema
	}

	return schemaMap, nil
}

func mysqlColumnTypeToValueType(ct string) (ColumnType, error) {
	ct = strings.ToLower(ct)
	if strings.HasPrefix(ct, "int") ||
		strings.HasPrefix(ct, "smallint") ||
		strings.HasPrefix(ct, "mediumint") ||
		strings.HasPrefix(ct, "bigint") {
		return ColumnTypeInt, nil
	}
	if strings.HasPrefix(ct, "float") ||
		strings.HasPrefix(ct, "double") ||
		strings.HasPrefix(ct, "decimal") {
		return ColumnTypeFloat, nil
	}
	if strings.HasPrefix(ct, "char") ||
		strings.HasPrefix(ct, "varchar") ||
		strings.HasPrefix(ct, "text") ||
		strings.HasPrefix(ct, "mediumtext") ||
		strings.HasPrefix(ct, "longtext") ||
		strings.HasPrefix(ct, "json") {
		return ColumnTypeString, nil
	}
	if strings.HasPrefix(ct, "datetime") ||
		strings.HasPrefix(ct, "timestamp") {
		return ColumnTypeDatetime, nil
	}
	if strings.HasPrefix(ct, "date") {
		return ColumnTypeDate, nil
	}
	if strings.HasPrefix(ct, "blob") {
		return ColumnTypeBytes, nil
	}
	return ColumnTypeNull, fmt.Errorf("convertion not found for MySQL type: %s", ct)
}

func (ds *MySQLDatasource) GetAllSchema(ctx context.Context) ([]*Schema, error) {
	allMap, err := ds.createAllSchemaMap()
	if err != nil {
		return nil, err
	}

	var all []*Schema
	for _, sc := range allMap {
		all = append(all, sc)
	}
	return all, nil
}

func (ds *MySQLDatasource) GetSchema(ctx context.Context, name string) (*Schema, error) {
	all, err := ds.createAllSchemaMap()
	if err != nil {
		return nil, err
	}
	for scName, sc := range all {
		if scName == name {
			return sc, nil
		}
	}
	return nil, errors.New("schema not found: " + name)
}

func (ds *MySQLDatasource) SetSchema(ctx context.Context, schema *Schema) error {
	return errors.New("not support SetSchema()")
}

func (ds *MySQLDatasource) GetRows(ctx context.Context, schema *Schema) ([]*Row, error) {
	// get data
	sqlRows, err := ds.db.Query(fmt.Sprintf("SELECT * FROM %s", schema.Name))
	if err != nil {
		return nil, err
	}
	defer sqlRows.Close()

	var rows []*Row
	for sqlRows.Next() {
		rowValues := make(RowValues)
		rowValuesGroupByKey := make(map[*Key][]*GenericColumnValue)
		ptrs := make([]interface{}, len(schema.Columns))
		for i, col := range schema.Columns {
			dvp := reflect.New(colToMySQLType(col)).Interface()
			ptrs[i] = dvp
		}
		if err := sqlRows.Scan(ptrs...); err != nil {
			return nil, err
		}
		for i, col := range schema.Columns {
			v := reflect.ValueOf(ptrs[i]).Elem().Interface()
			cv := &GenericColumnValue{Column: col, Value: v}
			rowValues[col.Name] = cv
			for _, name := range schema.PrimaryKey.ColumnNames {
				if name == col.Name {
					rowValuesGroupByKey[schema.PrimaryKey] = append(rowValuesGroupByKey[schema.PrimaryKey], cv)
				}
			}
		}
		rows = append(rows, &Row{rowValuesGroupByKey, rowValues})
	}
	return rows, nil
}

func (ds *MySQLDatasource) SetRows(ctx context.Context, schema *Schema, rows []*Row) error {
	return errors.New("MySQLDatasource does not support SetRows()")
}

func colToMySQLType(c *Column) reflect.Type {
	switch c.Type {
	case ColumnTypeInt:
		if c.NotNull {
			return reflect.TypeOf(int64(0))
		}
		return reflect.TypeOf(sql.NullInt64{})

	case ColumnTypeFloat:
		if c.NotNull {
			return reflect.TypeOf(float64(0))
		}
		return reflect.TypeOf(sql.NullFloat64{})
	case ColumnTypeBool:
		if c.NotNull {
			return reflect.TypeOf(false)
		}
		return reflect.TypeOf(sql.NullBool{})
	case ColumnTypeDatetime, ColumnTypeDate:
		if c.NotNull {
			return reflect.TypeOf(time.Time{})
		}
		return reflect.TypeOf(mysql.NullTime{})
	case ColumnTypeString:
		if c.NotNull {
			return reflect.TypeOf("")
		}
		return reflect.TypeOf(sql.NullString{})
	case ColumnTypeBytes:
		return reflect.TypeOf([]byte{})
	}
	return reflect.TypeOf(nil)
}
