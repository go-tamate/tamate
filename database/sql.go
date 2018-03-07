package database

import (
	"database/sql"
	"errors"
	"fmt"

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

func (db *SQLDatabase) dumpSQLTable(schema *schema.Schema) error {
	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", db.Server.User, db.Server.Password, db.Server.Host, db.Server.Port, db.Name)
	cnn, err := sql.Open(db.Server.DriverName, dataSourceName)
	if err != nil {
		return err
	}

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

func (db *SQLDatabase) Dump(schema *schema.Schema) error {
	return db.dumpSQLTable(schema)
}
