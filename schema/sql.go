package schema

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"strings"

	"github.com/Mitu217/tamate/config"

	_ "github.com/go-sql-driver/mysql"
)

// NewServerSchema :
func (sc *SQLSchema) NewServerSchema() error {
	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", sc.Server.User, sc.Server.Password, sc.Server.Host, sc.Server.Port, sc.DatabaseName)
	cnn, err := sql.Open(sc.Server.DriverName, dataSourceName)
	if err != nil {
		return err
	}

	// Get data
	rows, err := cnn.Query("SHOW COLUMNS FROM " + sc.Table.Name)
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
		return errors.New("No columns in table " + sc.Table.Name + ".")
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
					if strings.Index(value.String, "PRI") != -1 {
						// FIXME: column.nameが空になる場合の対応
						sc.Table.PrimaryKey = column.Name
					}
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

// GetColumns :
func (sc *SQLSchema) GetColumns() []Column {
	return sc.Columns
}

// GetTableName :
func (sc *SQLSchema) GetTableName() string {
	return sc.Table.Name
}

// Output :
func (sc *SQLSchema) Output(path string) error {
	// Set default path and default file name.
	if path == "" {
		path = "resources/schema/" + sc.Server.Host + "_" + sc.DatabaseName + "_" + sc.Table.Name + ".json"
	}

	// Output with indentation
	jsonBytes, err := json.MarshalIndent(sc, "", "  ")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(path, jsonBytes, 0644)
}

// SQLSchema :
type SQLSchema struct {
	Server       *config.ServerConfig `json:"server"`
	DatabaseName string               `json:"database"`
	Description  string               `json:"description"`
	Table        Table                `json:"table"`
	Columns      []Column             `json:"properties"`
}
