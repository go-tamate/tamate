package schema

import (
	"database/sql"
	"fmt"
	"strings"
)

// Column :
type Column struct {
	Name          string `json:"name"`
	Type          string `json:"type"`
	NotNull       bool   `json:"not_null"`
	AutoIncrement bool   `json:"auto_increment"`
}

// schema :
type Schema struct {
	Name       string   `json:"name"`
	PrimaryKey string   `json:"primary_key"`
	Columns    []Column `json:"columns"`
}

func (sc *Schema) ColumnNames() []string {
	var colNames []string
	for _, col := range sc.Columns {
		colNames = append(colNames, col.Name)
	}
	return colNames
}

func (sc *Schema) HasColumn(name string) bool {
	for _, col := range sc.Columns {
		if col.Name == name {
			return true
		}
	}
	return false
}

func (sc *Schema) ColumnIndex(colName string) int {
	for i, col := range sc.Columns {
		if col.Name == colName {
			return i
		}
	}
	return -1
}

// All of type is as "string"
func NewSchemaFromRow(tableName string, row []string) (*Schema, error) {
	var cols []Column
	for _, colName := range row {
		cols = append(cols, Column{
			Name:          colName,
			Type:          "string",
			NotNull:       true,
			AutoIncrement: false,
		})
	}
	return &Schema{
		Name:    tableName,
		Columns: cols,
	}, nil
}

// TODO: test
func NewSchemaFromMySQL(dsn, tableName string) (*Schema, error) {
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	defer db.Close()

	// Get data
	rows, err := db.Query("SHOW COLUMNS FROM " + tableName)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Get columns
	sqlColumns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	if len(sqlColumns) == 0 {
		return nil, fmt.Errorf("no columns in table %s", tableName)
	}

	// Read data
	sc := &Schema{
		Name: tableName,
	}

	for rows.Next() {
		var field string
		var type_ string
		var null string
		var key string
		var default_ string
		var extra string

		// Read data
		if err := rows.Scan(&field, &type_, &null, &key, &default_, &extra); err != nil {
			return nil, err
		}

		col := Column{
			Name:          field,
			Type:          type_,
			NotNull:       null != "YES",
			AutoIncrement: strings.Contains(extra, "auto_increment"),
		}
		if strings.Contains(key, "PRIMARY") {
			sc.PrimaryKey = col.Name
		}
		sc.Columns = append(sc.Columns, col)
	}
	return sc, nil
}
