package datasource

import (
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"github.com/Mitu217/tamate/config"
	"github.com/Mitu217/tamate/schema"

	// MySQL Driver
	_ "github.com/go-sql-driver/mysql"
)

// SQLDataSource :
type SQLDataSource struct {
	Config *config.SQLConfig
	Schema *schema.Schema
}

// NewSQLDataSource :
func NewSQLDataSource(config *config.SQLConfig) (*SQLDataSource, error) {
	ds := &SQLDataSource{
		Config: config,
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
func (ds *SQLDataSource) GetSchema() (*schema.Schema, error) {
	if ds.Schema != nil {
		return ds.Schema, nil
	}

	cnn, err := ds.open()
	if err != nil {
		return nil, err
	}

	// Get data
	rows, err := cnn.Query("SHOW COLUMNS FROM " + ds.Config.TableName)
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
		return nil, errors.New("No columns in table " + ds.Config.TableName + ".")
	}

	// Read data
	sc := &schema.Schema{
		DatabaseName: ds.Config.DatabaseName,
		Table: schema.Table{
			Name: ds.Config.TableName,
		},
	}
	for rows.Next() {
		data := make([]*sql.NullString, len(sqlColumns))
		ptrs := make([]interface{}, len(sqlColumns))
		for i := range data {
			ptrs[i] = &data[i]
		}

		// Read data
		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}

		var column schema.Column
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
		sc.Columns = append(sc.Columns, column)
	}
	ds.Schema = sc
	return sc, nil
}

// GetRows :
func (ds *SQLDataSource) GetRows() (*Rows, error) {
	cnn, err := ds.open()
	if err != nil {
		return nil, err
	}
	sc, err := ds.GetSchema()
	if err != nil {
		return nil, err
	}
	// Get data
	sqlRows, err := cnn.Query("SELECT * FROM " + sc.Table.Name)
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
		return nil, errors.New("No columns in table " + sc.Table.Name + ".")
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
	sc, err := ds.GetSchema()
	if err != nil {
		return err
	}

	columns := make([]string, 0)
	for _, column := range sc.Columns {
		columns = append(columns, column.Name)
	}
	columnsText := strings.Join(columns, ",")

	data := rows.Values
	values := make([]string, len(data))
	for i := range data {
		valueText := make([]string, len(data[i]))
		for j := range data[i] {
			if sc.Columns[j].Type == "int" {
				valueText[j] = data[i][j]
			}
			valueText[j] = "'" + data[i][j] + "'"
		}
		values[i] = "(" + strings.Join(valueText, ",") + ")"
	}
	valuesText := strings.Join(values, ",")

	// Insert data
	_, err = cnn.Query("INSERT INTO " + sc.Table.Name + " (" + columnsText + ") VALUES " + valuesText)
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
	cnn.Query("TRUNCATE TABLE " + sc.Table.Name)

	return nil
}
