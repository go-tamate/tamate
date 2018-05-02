package table

import (
	"database/sql"
	"errors"
	// TODO: fix depends on mysql https://github.com/Mitu217/tamate/issues/44
	_ "github.com/go-sql-driver/mysql"

	"github.com/Mitu217/tamate/table/config"
	"github.com/Mitu217/tamate/table/schema"
)

// SQLTable :
type SQLTable struct {
	Schema *schema.Schema         `json:"schema"`
	Config *config.SQLTableConfig `json:"config"`
	db     *sql.DB
}

// NewSQL :
func NewSQL(sc *schema.Schema, conf *config.SQLTableConfig) (*SQLTable, error) {
	ds := &SQLTable{
		Schema: sc,
		Config: conf,
	}
	if err := ds.Open(); err != nil {
		return nil, err
	}
	return ds, nil
}

func (tbl *SQLTable) Open() error {
	db, err := sql.Open(tbl.Config.DriverName, tbl.Config.DSN)
	if err != nil {
		return err
	}
	if err := db.Ping(); err != nil {
		return err
	}
	tbl.db = db
	return nil
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

	if err := tbl.Open(); err != nil {
		return nil, err
	}

	// Get data
	sqlRows, err := tbl.db.Query("SELECT * FROM " + sc.Name)
	if err != nil {
		return nil, err
	}
	defer sqlRows.Close()

	tbl.Close()

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
