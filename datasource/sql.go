package datasource

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/Mitu217/tamate/schema"

	_ "github.com/go-sql-driver/mysql"
)

// SQLConfig :
type SQLConfig struct {
	Server       *schema.Server
	DatabaseName string
	TableName    string
}

// SQLDataSource :
type SQLDataSource struct {
	Config *SQLConfig
	Schema schema.Schema
}

// NewJSONSQLConfig :
func NewJSONSQLConfig(jsonPath string, dbName string, tableName string) (*SQLConfig, error) {
	var sv *schema.Server
	r, err := os.Open(jsonPath)
	if err != nil {
		return nil, err
	}

	if err := json.NewDecoder(r).Decode(&sv); err != nil {
		return nil, err
	}
	config := &SQLConfig{
		Server:       sv,
		DatabaseName: dbName,
		TableName:    tableName,
	}
	return config, nil
}

// NewSQLDataSource :
func NewSQLDataSource(sc schema.Schema, config *SQLConfig) (*SQLDataSource, error) {
	ds := &SQLDataSource{
		Config: config,
		Schema: sc,
	}
	return ds, nil
}

func (ds *SQLDataSource) open() (*sql.DB, error) {
	user := ds.Config.Server.User
	pw := ds.Config.Server.Password
	host := ds.Config.Server.Host
	port := ds.Config.Server.Port
	dbName := ds.Config.DatabaseName
	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", user, pw, host, port, dbName)
	return sql.Open(ds.Config.Server.DriverName, dataSourceName)
}

// GetSchema :
func (ds *SQLDataSource) GetSchema() (schema.Schema, error) {
	return ds.Schema, nil
}

// GetRows :
func (ds *SQLDataSource) GetRows() (*Rows, error) {
	cnn, err := ds.open()

	// Get data
	sqlRows, err := cnn.Query("SELECT * FROM " + ds.Schema.GetTableName())
	if err != nil {
		return nil, err
	}
	defer sqlRows.Close()

	// Get columns
	columns, err := sqlRows.Columns()
	if err != nil {
		return nil, err
	}
	if len(columns) == 0 {
		return nil, errors.New("No columns in table " + ds.Schema.GetTableName() + ".")
	}

	// Read data
	records := make([][]string, 0)
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

		dataStrings := make([]string, len(columns))

		for key, value := range data {
			if value != nil && value.Valid {
				dataStrings[key] = value.String
			}
		}

		records = append(records, dataStrings)
	}

	rows := &Rows{
		Columns: columns,
		Values:  records,
	}
	return rows, nil
}

// SetRows :
func (ds *SQLDataSource) SetRows(rows *Rows) error {
	cnn, err := ds.open()
	if err != nil {
		return err
	}

	columns := make([]string, 0)
	for _, column := range ds.Schema.GetColumns() {
		columns = append(columns, column.Name)
	}
	columnsText := strings.Join(columns, ",")

	data := rows.Values
	values := make([]string, len(data))
	for i := range data {
		valueText := make([]string, len(data[i]))
		for j := range data[i] {
			if ds.Schema.GetColumns()[j].Type == "int" {
				valueText[j] = data[i][j]
			}
			valueText[j] = "'" + data[i][j] + "'"
		}
		values[i] = "(" + strings.Join(valueText, ",") + ")"
	}
	valuesText := strings.Join(values, ",")

	// Insert data
	_, err = cnn.Query("INSERT INTO " + ds.Config.TableName + " (" + columnsText + ") VALUES " + valuesText)
	if err != nil {
		return err
	}
	return nil
}

func (ds *SQLDataSource) resetSQLTable(sc schema.Schema) error {
	cnn, err := ds.open()
	if err != nil {
		return err
	}

	// Truncate data
	cnn.Query("TRUNCATE TABLE " + sc.GetTableName())

	return nil
}
