package tamate

import (
	"context"
	"fmt"

	"github.com/go-tamate/tamate/driver"
)

var (
	drivers = make(map[string]driver.Driver)
)

type Rows struct {
	rowsi    driver.Rows
	lastcols []driver.Value
	lasterr  error
}

func (rs *Rows) Next() bool {
	if rs.lastcols == nil {
		rs.lastcols = make([]driver.Value, len(rs.rowsi.Columns()))
	}
	rs.lasterr = rs.rowsi.Next(rs.lastcols)
	if rs.lasterr != nil {
		return false
	}
	return true
}

func (rs *Rows) GetRow() ([]driver.Value, error) {
	if rs.lastcols == nil {
		return nil, errors.New("tamate: GetRow called without calling Next")
	}

	dest := make([]driver.Value, len(rs.lastcols))
	for i := range rs.lastcols {
		if err := convertAssign(&dest[i], rs.lastcols[i]); err != nil {
			return nil, err
		}
	}
	return dest, nil
}

func (rs *Rows) Close() error {
	return rs.rowsi.Close()
}

type DataSource struct {
	connector  driver.Connector
	driverConn driver.Conn
	stop       func()
}

func (ds *DataSource) GetSchema(ctx context.Context, name string) (*driver.Schema, error) {
	return ds.driverConn.GetSchema(ctx, name)
}

func (ds *DataSource) SetSchema(ctx context.Context, name string, schema *driver.Schema) error {
	return ds.driverConn.SetSchema(ctx, name, schema)
}

func (ds *DataSource) GetRows(ctx context.Context, name string) (*Rows, error) {
	rowsi, err := ds.driverConn.GetRows(ctx, name)
	if err != nil {
		return nil, err
	}
	return &Rows{
		rowsi: rowsi,
	}, nil
}

func (ds *DataSource) SetRows(ctx context.Context, name string, rowsValues [][]driver.Value) error {
	return ds.driverConn.SetRows(ctx, name, rowsValues)
}

func (ds *DataSource) Close() error {
	return ds.driverConn.Close()
}

type dsnConnector struct {
	dsn    string
	driver driver.Driver
}

func (c *dsnConnector) Connect(ctx context.Context) (driver.Conn, error) {
	return c.driver.Open(ctx, c.dsn)
}

func (c *dsnConnector) Driver() driver.Driver {
	return c.driver
}

func Register(name string, driver driver.Driver) {
	if driver == nil {
		panic("tamate: Register driver is nil")
	}
	if _, dup := drivers[name]; dup {
		panic("tamate: Register called twice for driver " + name)
	}
	drivers[name] = driver
}

func Drivers() map[string]driver.Driver {
	return drivers
}

func Open(name string, dsn string) (*DataSource, error) {
	driveri, ok := drivers[name]
	if !ok {
		return nil, fmt.Errorf("tamate: unknown datasource %q (forgotten import?)", name)
	}

	return OpenDataSource(&dsnConnector{dsn: dsn, driver: driveri})
}

func OpenDataSource(connector driver.Connector) (*DataSource, error) {
	ctx, cancel := context.WithCancel(context.Background())

	dataSource := &DataSource{
		connector: connector,
		stop:      cancel,
	}

	driverConn, err := connector.Connect(ctx)
	if err != nil {
		return nil, err
	}
	dataSource.driverConn = driverConn

	return dataSource, nil
}
