package datasource

import (
	_ "github.com/go-sql-driver/mysql"
	"testing"
)

var mysqlConfig = &SQLDatasourceConfig{
	Type:         "sql",
	DriverName:   "mysql",
	DSN:          "root:password@/information_schema",
	DatabaseName: "information_schema",
	TableName:    "tables",
}

func TestMySQL(t *testing.T) {
	ds, err := NewSQLDataSource(mysqlConfig)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := ds.GetRows()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("count of information_schema.tables rows: %d", len(rows.Values))
}
