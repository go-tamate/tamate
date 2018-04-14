package table

import (
	"database/sql"
	"errors"
	// TODO: fix depends on mysql https://github.com/Mitu217/tamate/issues/44
	_ "github.com/go-sql-driver/mysql"

	"github.com/Mitu217/tamate/table/schema"
)

// SQLConfig :
type SQLTableConfig struct {
	DriverName string `json:"driver_name"`
	DSN        string `json:"dsn"`
	TableName  string `json:"table_name"`
}

// SQLTable :
type SQLTable struct {
	Schema *schema.Schema  `json:"schema"`
	Config *SQLTableConfig `json:"config"`
	db     *sql.DB
}

// NewSQL :
func NewSQL(sc *schema.Schema, conf *SQLTableConfig) (*SQLTable, error) {
	db, err := sql.Open(conf.DriverName, conf.DSN)
	if err != nil {
		return nil, err
	}
	if err := db.Ping(); err != nil {
		return nil, err
	}
	ds := &SQLTable{
		Schema: sc,
		Config: conf,
		db:     db,
	}
	return ds, nil
}

func (tbl *SQLTable) Close() error {
	if tbl.db != nil {
		return tbl.db.Close()
	}
	return nil
}

// GetSchema :
func (tbl *SQLTable) GetSchema() (*schema.Schema, error) {
	return tbl.Schema, nil
}

// GetRows :
func (tbl *SQLTable) GetRows() (*Rows, error) {
	sc, err := tbl.GetSchema()
	if err != nil {
		return nil, err
	}
	// Get data
	sqlRows, err := tbl.db.Query("SELECT * FROM " + sc.Name)
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
		return nil, errors.New("No columns in table " + sc.Name + ".")
	}

	// Read data
	var records [][]string
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
		Values: records,
	}
	return rows, nil
}
