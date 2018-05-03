package handler

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	// mysql driver
	_ "github.com/go-sql-driver/mysql"
)

// SQLHandler is handler struct of sql
type SQLHandler struct {
	DriverName string `json:"driver_name"`
	DSN        string `json:"dsn"`
	db         *sql.DB
}

// NewSQLHandler is create SQLHandler instance method
func NewSQLHandler(driverName string, dsn string) (*SQLHandler, error) {
	return &SQLHandler{
		DriverName: driverName,
		DSN:        dsn,
	}, nil
}

// Open is call by datasource when create instance
func (h *SQLHandler) Open() error {
	if h.db == nil {
		db, err := sql.Open(h.DriverName, h.DSN)
		if err != nil {
			return err
		}
		if err := db.Ping(); err != nil {
			return err
		}
		h.db = db
	}
	return nil
}

// Close is call by datasource when free instance
func (h *SQLHandler) Close() error {
	if h.db != nil {
		err := h.db.Close()
		h.db = nil
		if err != nil {
			return err
		}
	}
	return nil
}

// GetSchemas is get all schemas method
func (h *SQLHandler) GetSchemas() (*[]Schema, error) {
	// get schemas
	sqlRows, err := h.db.Query("SELECT TABLE_NAME, COLUMN_NAME, COLUMN_TYPE, IS_NULLABLE, COLUMN_KEY, COLUMN_DEFAULT, EXTRA FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = DATABASE()")
	if err != nil {
		return nil, err
	}
	defer sqlRows.Close()

	columns, err := sqlRows.Columns()
	if err != nil {
		return nil, err
	}
	schemaMap := make(map[string]Schema)
	for sqlRows.Next() {
		data := make([]*sql.NullString, len(columns))
		ptrs := make([]interface{}, len(columns))
		for i := range data {
			ptrs[i] = &data[i]
		}
		// Read data
		if err := sqlRows.Scan(ptrs...); err != nil {
			return nil, err
		}
		tableName := data[0].String
		columnName := data[1].String
		columnType := data[2].String
		isNullable := data[3].String == "true"
		// init schema struct when first
		_, ok := schemaMap[tableName]
		if !ok {
			schemaMap[tableName] = Schema{
				Name: tableName,
			}
		}
		schema := schemaMap[tableName]
		column := Column{
			Name:    columnName,
			Type:    columnType,
			NotNull: !isNullable,
		}
		schema.Columns = append(schema.Columns, column)
		schemaMap[tableName] = schema
	}
	schemas := []Schema{}
	for tableName := range schemaMap {
		schemas = append(schemas, schemaMap[tableName])
	}
	return &schemas, nil
}

// GetSchema is get schema method
func (h *SQLHandler) GetSchema(schema *Schema) error {
	return errors.New("not support GetSchema()")
}

// SetSchema is set schema method
func (h *SQLHandler) SetSchema(schema *Schema) error {
	return errors.New("not support SetSchema()")
}

// GetRows is get rows method
func (h *SQLHandler) GetRows(schema *Schema) (*Rows, error) {
	// get data
	sqlRows, err := h.db.Query(fmt.Sprintf("SELECT * FROM %s", schema.Name))
	if err != nil {
		return nil, err
	}
	defer sqlRows.Close()

	// read data
	columnLength := len(schema.Columns)
	var records [][]string
	for sqlRows.Next() {
		data := make([]*sql.NullString, columnLength)
		ptrs := make([]interface{}, columnLength)
		for i := range data {
			ptrs[i] = &data[i]
		}

		// Read data
		if err := sqlRows.Scan(ptrs...); err != nil {
			return nil, err
		}

		dataStrings := make([]string, columnLength)

		for key, value := range data {
			if value != nil && value.Valid {
				dataStrings[key] = value.String
			}
		}

		records = append(records, dataStrings)
	}
	rows := &Rows{
		Values: records,
	}
	return rows, nil
}

// SetRows is set rows method
func (h *SQLHandler) SetRows(schema *Schema, rows *Rows) error {
	// reset table
	sqlRows, err := h.db.Query(fmt.Sprintf("DELETE FROM %s", schema.Name))
	if err != nil {
		return err
	}
	defer sqlRows.Close()

	// write data
	columns := make([]string, 0)
	for _, column := range schema.Columns {
		columns = append(columns, column.Name)
	}
	columnsText := strings.Join(columns, ",")

	data := rows.Values
	values := make([]string, len(data))
	for i := range data {
		valueText := make([]string, len(data[i]))
		for j := range data[i] {
			if schema.Columns[j].Type == "int" {
				valueText[j] = data[i][j]
			}
			valueText[j] = "'" + data[i][j] + "'"
		}
		values[i] = "(" + strings.Join(valueText, ",") + ")"
	}
	valuesText := strings.Join(values, ",")

	sqlRows, err = h.db.Query(fmt.Sprintf("INSERT INTO %s (%s) VALUES %s", schema.Name, columnsText, valuesText))
	if err != nil {
		return err
	}
	defer sqlRows.Close()
	return nil
}
