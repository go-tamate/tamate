package datasource

import (
	"database/sql"
	"errors"
	"strings"

	// TODO: fix depends on mysql https://github.com/Mitu217/tamate/issues/44
	_ "github.com/go-sql-driver/mysql"

	"github.com/Mitu217/tamate/schema"
)

// SQLConfig :
type SQLDatasourceConfig struct {
	Type         string `json:"type"`
	DriverName   string `json:"driver_name"`
	DSN          string `json:"dsn"`
	DatabaseName string `json:"database_name"`
	TableName    string `json:"table_name"`
}

// SQLDataSource :
type SQLDataSource struct {
	db     *sql.DB
	Config *SQLDatasourceConfig
	Schema *schema.Schema
}

// NewSQLDataSource :
func NewSQLDataSource(config *SQLDatasourceConfig) (*SQLDataSource, error) {
	db, err := sql.Open(config.DriverName, config.DSN)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	ds := &SQLDataSource{
		db:     db,
		Config: config,
	}
	return ds, nil
}

func (ds *SQLDataSource) Close() error {
	if ds.db != nil {
		return ds.db.Close()
	}
	return nil
}

// GetSchema :
func (ds *SQLDataSource) GetSchema() (*schema.Schema, error) {
	if ds.Schema != nil {
		return ds.Schema, nil
	}

	// Get data
	rows, err := ds.db.Query("SHOW COLUMNS FROM " + ds.Config.TableName)
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

// SetSchema :
func (ds *SQLDataSource) SetSchema(sc *schema.Schema) error {
	ds.Schema = sc
	return nil
}

// GetRows :
func (ds *SQLDataSource) GetRows() (*Rows, error) {
	sc, err := ds.GetSchema()
	if err != nil {
		return nil, err
	}
	// Get data
	sqlRows, err := ds.db.Query("SELECT * FROM " + sc.Table.Name)
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
