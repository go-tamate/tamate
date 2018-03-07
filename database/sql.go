package database

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/Mitu217/tamate/schema"

	_ "github.com/go-sql-driver/mysql"
)

type Table struct {
	Columns []string
	Records [][]string
}

type SQLDatabase struct {
	Server *Server
	Name   string
	Tables []*Table
}

func (db *SQLDatabase) open() (*sql.DB, error) {
	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", db.Server.User, db.Server.Password, db.Server.Host, db.Server.Port, db.Name)
	return sql.Open(db.Server.DriverName, dataSourceName)
}

func (db *SQLDatabase) dumpSQLTable(schema *schema.Schema) error {
	cnn, err := db.open()

	// Get data
	rows, err := cnn.Query("SELECT * FROM " + schema.Table.Name)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Get columns
	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	if len(columns) == 0 {
		return errors.New("No columns in table " + schema.Table.Name + ".")
	}

	// Read data
	records := make([][]string, 0)
	for rows.Next() {
		data := make([]*sql.NullString, len(columns))
		ptrs := make([]interface{}, len(columns))
		for i := range data {
			ptrs[i] = &data[i]
		}

		// Read data
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}

		dataStrings := make([]string, len(columns))

		for key, value := range data {
			if value != nil && value.Valid {
				dataStrings[key] = value.String
			}
		}

		records = append(records, dataStrings)
	}

	table := &Table{
		Columns: columns,
		Records: records,
	}
	db.Tables = append(db.Tables, table)
	return nil
}

func (db *SQLDatabase) resetSQLTable(schema *schema.Schema) error {
	cnn, err := db.open()
	if err != nil {
		return err
	}

	// Truncate data
	cnn.Query("TRUNCATE TABLE " + schema.Table.Name)

	return nil
}

func (db *SQLDatabase) restoreSQLTable(schema *schema.Schema, data [][]interface{}) error {
	cnn, err := db.open()
	if err != nil {
		return err
	}

	columns := make([]string, 0)
	for _, column := range schema.Properties {
		columns = append(columns, column.Name)
	}
	columns_text := strings.Join(columns, ",")

	values := make([]string, len(data))
	for i := range data {
		value_text := make([]string, len(data[i]))
		for j := range data[i] {
			if schema.Properties[j].Type == "int" {
				value_text[j] = data[i][j].(string)
			}
			value_text[j] = "'" + data[i][j].(string) + "'"
		}
		values[i] = "(" + strings.Join(value_text, ",") + ")"
	}
	values_text := strings.Join(values, ",")

	// Truncate data
	_, err = cnn.Query("INSERT INTO " + schema.Table.Name + " (" + columns_text + ") VALUES " + values_text)
	if err != nil {
		return err
	}

	return nil
}

func (db *SQLDatabase) Dump(schema *schema.Schema) error {
	return db.dumpSQLTable(schema)
}

func (db *SQLDatabase) Restore(schema *schema.Schema, data [][]interface{}) error {
	err := db.resetSQLTable(schema)
	if err != nil {
		return err
	}
	return db.restoreSQLTable(schema, data)
}
