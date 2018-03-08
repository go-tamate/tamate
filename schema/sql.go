package schema

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/Mitu217/tamate/server"

	_ "github.com/go-sql-driver/mysql"
)

func (sc *SQLSchema) NewServerSchema(tableName string) error {
	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", sc.Server.User, sc.Server.Password, sc.Server.Host, sc.Server.Port, sc.DatabaseName)
	cnn, err := sql.Open(sc.Server.DriverName, dataSourceName)
	if err != nil {
		return err
	}

	// Get data
	rows, err := cnn.Query("SHOW COLUMNS FROM " + tableName)
	if err != nil {
		return err
	}
	defer rows.Close()

	// Get columns
	sqlColumns, err := rows.Columns()
	if err != nil {
		return err
	}
	if len(sqlColumns) == 0 {
		return errors.New("No columns in table " + tableName + ".")
	}

	// Read data
	columns := make([]Column, 0)
	for rows.Next() {
		data := make([]*sql.NullString, len(sqlColumns))
		ptrs := make([]interface{}, len(sqlColumns))
		for i := range data {
			ptrs[i] = &data[i]
		}

		// Read data
		if err := rows.Scan(ptrs...); err != nil {
			return err
		}

		var column Column
		for key, value := range data {
			if value != nil && value.Valid {
				switch sqlColumns[key] {
				case "Field":
					column.Name = value.String
				case "Type":
					column.Type = value.String
				case "Null":
					if value.String == "YES" {
						column.NotNull = false
					} else {
						column.NotNull = true
					}
				case "Key":
					//property.Key = value.String
				case "Default":
					//property.Default = value.String
				case "Extra":
					if strings.Index(value.String, "auto_increment") != -1 {
						column.AutoIncrement = true
					}
				}
			}
		}
		columns = append(columns, column)
	}
	sc.Columns = columns
	return nil
}

func (sc *SQLSchema) GetColumns() []Column {
	return sc.Columns
}

func (sc *SQLSchema) GetTableName() string {
	return sc.Table.Name
}

func (sc *SQLSchema) Output(path string) error {
	// Output with indentation
	jsonBytes, err := json.MarshalIndent(sc, "", "  ")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(path, jsonBytes, 0644)
}

type SQLSchema struct {
	Server       *server.Server `json:"server"`
	DatabaseName string         `json:"database"`
	Description  string         `json:"description"`
	Table        Table          `json:"table"`
	Columns      []Column       `json:"properties"`
}
