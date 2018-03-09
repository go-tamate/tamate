package datasource

import (
	"database/sql"
	"errors"
	"fmt"

	"github.com/Mitu217/tamate/schema"
	"github.com/Mitu217/tamate/server"

	_ "github.com/go-sql-driver/mysql"
)

type Table struct {
	Columns []string
	Records [][]string
}

type SQLDataSource struct {
	Server       *server.Server
	DatabaseName string
	TableName    string
	Columns      []string
	Values       [][]string
}

func (ds *SQLDataSource) open() (*sql.DB, error) {
	dataSourceName := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s", ds.Server.User, ds.Server.Password, ds.Server.Host, ds.Server.Port, ds.DatabaseName)
	return sql.Open(ds.Server.DriverName, dataSourceName)
}

func (ds *SQLDataSource) dumpSQLTable(sc schema.Schema) error {
	cnn, err := ds.open()

	// Get data
	rows, err := cnn.Query("SELECT * FROM " + sc.GetTableName())
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
		return errors.New("No columns in table " + sc.GetTableName() + ".")
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
	ds.Columns = columns
	ds.Values = records

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

func (ds *SQLDataSource) restoreSQLTable(sc schema.Schema, data [][]interface{}) error {
	/*
		cnn, err := db.open()
		if err != nil {
			return err
		}

		columns := make([]string, 0)
		for _, column := range sc.GetColumns() {
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

		// Insert data
		_, err = cnn.Query("INSERT INTO " + schema.Table.Name + " (" + columns_text + ") VALUES " + values_text)
		if err != nil {
			return err
		}
	*/
	return nil
}

/*
func (ds *SQLDataSource) OutputCSV(sc schema.Schema, path string, columns []string, values [][]string) error {
	values = append([][]string{columns}, values...) // TODO: 遅いので修正する（https://mattn.kaoriya.net/software/lang/go/20150928144704.htm）
	return Output(path, values)
}
*/

func (ds *SQLDataSource) Dump(schema schema.Schema) error {
	return ds.dumpSQLTable(schema)
}

func (ds *SQLDataSource) Restore(schema *schema.Schema, data [][]interface{}) error {
	/*
		err := db.resetSQLTable(schema)
		if err != nil {
			return err
		}
		return db.restoreSQLTable(schema, data)
	*/
	return nil
}
