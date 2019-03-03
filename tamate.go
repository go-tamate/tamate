package tamate

import (
	"context"
	"fmt"
	"sync"

	"github.com/Mitu217/tamate/driver"
)

var (
	driversMu sync.RWMutex
	drivers   = make(map[string]driver.Driver)
)

type DataSource struct {
	connector  driver.Connector
	driverConn driver.Conn
	stop       func()
}

func (ds *DataSource) DriverConn() driver.Conn {
	return ds.driverConn
}

func (ds *DataSource) Close() error {
	return ds.driverConn.Close()
}

func Register(name string, driver driver.Driver) {
	driversMu.Lock()
	defer driversMu.Unlock()
	if driver == nil {
		panic("tamate: Register driver is nil")
	}
	if _, dup := drivers[name]; dup {
		panic("tamate: Register called twice for driver " + name)
	}
	drivers[name] = driver
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

func Open(name string, dsn string) (*DataSource, error) {
	driversMu.RLock()
	driveri, ok := drivers[name]
	driversMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("tamate: unknown datasource %q (forgotten import?)", name)
	}

	return OpenDataSource(&dsnConnector{dsn: dsn, driver: driveri}), nil
}

func OpenDataSource(connector driver.Connector) *DataSource {
	ctx, cancel := context.WithCancel(context.Background())

	dataSource := &DataSource{
		connector: connector,
		stop:      cancel,
	}

	// TODO: goroutine safe
	driverConn, err := connector.Connect(ctx)
	if err != nil {
		panic(err)
	}

	dataSource.driverConn = driverConn
	return dataSource
}
