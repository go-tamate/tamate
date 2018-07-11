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

// MySQLDatasource is datasource config for MySQL DB
type MySQLDatasource struct {
	DSN string `json:"dsn"`
	db  *sql.DB
}

// NewMySQLDatasource is create MySQLDatasource instance
func NewMySQLDatasource(dsn string) (*MySQLDatasource, error) {
	return &MySQLDatasource{
		DSN: dsn,
	}, nil
}

// Open is open connection
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

// Close is close connection
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

// GetSchema is getting schema from MySQL DB
func (ds *MySQLDatasource) GetSchema(ctx context.Context, name string) (*Schema, error) {
	schemaMap, err := ds.getSchemaMap()
	if err != nil {
		return nil, err
	}
	for scName, sc := range schemaMap {
		if scName == name {
			return sc, nil
		}
	}
	return nil, errors.New("schema not found: " + name)
}

// GetRows is getting rows from MySQL DB
func (ds *MySQLDatasource) GetRows(ctx context.Context, schema *Schema) ([]*Row, error) {
	result, err := ds.db.Query(fmt.Sprintf("SELECT * FROM %s", schema.Name))
	if err != nil {
		return nil, err
	}
	defer result.Close()

	var rows []*Row
	for result.Next() {
		rowValues := make(RowValues, len(schema.Columns))
		rowValuesGroupByKey := make(GroupByKey)
		ptrs := make([]interface{}, len(schema.Columns))
		for i, col := range schema.Columns {
			ptr := reflect.New(colToMySQLType(col)).Interface()
			ptrs[i] = ptr
		}
		if err := result.Scan(ptrs...); err != nil {
			return nil, err
		}
		for i, col := range schema.Columns {
			val := reflect.ValueOf(ptrs[i]).Elem().Interface()
			colValue := &GenericColumnValue{Column: col, Value: val}
			rowValues[col.Name] = colValue
			for i := range schema.PrimaryKey.ColumnNames {
				if schema.PrimaryKey.ColumnNames[i] == col.Name {
					key := schema.PrimaryKey.String()
					rowValuesGroupByKey[key] = append(rowValuesGroupByKey[key], colValue)
				}
			}
		}
		rows = append(rows, &Row{GroupByKey: rowValuesGroupByKey, Values: rowValues})
	}
	return rows, nil
}

// SetSchema is setting schema to MySQL DB
func (ds *MySQLDatasource) SetSchema(ctx context.Context, schema *Schema) error {
	return fmt.Errorf("feature support")
}

// SetRows is setting rows to MySQL DB
func (ds *MySQLDatasource) SetRows(ctx context.Context, schema *Schema, rows []*Row) error {
	return fmt.Errorf("feature support")
}

func (ds *MySQLDatasource) getSchemaMap() (map[string]*Schema, error) {
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
					KeyType: KeyTypePrimary,
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
