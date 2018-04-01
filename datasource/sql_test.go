package datasource

import (
	"database/sql"
	"fmt"
	dockertest "gopkg.in/ory-am/dockertest.v3"
	"log"
	"os"
	"testing"
)

var mysqlConfig *SQLDatasourceConfig

func TestMain(m *testing.M) {
	// uses a sensible default on windows (tcp/http) and linux/osx (socket)
	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	// pulls an image, creates a container based on it and runs it
	resource, err := pool.Run("mysql", "5.7", []string{"MYSQL_ROOT_PASSWORD=password"})
	if err != nil {
		log.Fatalf("Could not start resource: %s", err)
	}

	// exponential backoff-retry, because the application in the container might not be ready to accept connections yet
	if err := pool.Retry(func() error {
		driverName := "mysql"
		dbName := "information_schema"
		dsn := fmt.Sprintf("root:password@(localhost:%s)/%s", resource.GetPort("3306/tcp"), dbName)
		mysqlConfig = &SQLDatasourceConfig{
			Type:         "sql",
			DriverName:   driverName,
			DSN:          dsn,
			DatabaseName: dbName,
			TableName:    "tables",
		}

		// ping db
		db, err := sql.Open(driverName, dsn)
		if err != nil {
			return err
		}
		return db.Ping()
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	code := m.Run()

	// You can't defer this because os.Exit doesn't care for defer
	if err := pool.Purge(resource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}

	os.Exit(code)
}

func TestMySQL(t *testing.T) {
	ds, err := NewSQLDataSource(mysqlConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()
	rows, err := ds.GetRows()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Values) < 1 {
		t.Fatalf("count of tables on information_schema.tables shoud be > 0")
	}
}
