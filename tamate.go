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
	connector driver.Connector
}

func (ds *DataSource) Export(dataSource *DataSource) error {
	return nil
}

func (ds *DataSource) Import(dataSource *DataSource) error {
	return nil
}

func (ds *DataSource) Diff(datasource *DataSource) error {
	return nil
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

func Open(name string, dsn string) (*DataSource, error) {
	driversMu.RLock()
	driveri, ok := drivers[name]
	driversMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("tamate: unknown datasource %q (forgotten import?)", name)
	}

	return OpenDataSource(&dsnConnector{dsn: dsn, driver: driveri}), nil
}

type dsnConnector struct {
	dsn    string
	driver driver.Driver
}

func (c *dsnConnector) Connect(ctx context.Context) (driver.Conn, error) {
	return c.driver.Open(ctx, c.dsn)
}

func OpenDataSource(connector driver.Connector) *DataSource {
	//ctx, cancel := context.WithCancel(context.Background())
	datasource := &DataSource{
		connector: connector,
		/*
			openerCh:     make(chan struct{}, connectionRequestQueueSize),
			resetterCh:   make(chan *driverConn, 50),
			lastPut:      make(map[*driverConn]string),
			connRequests: make(map[uint64]chan connRequest),
			stop:         cancel,
		*/
	}

	//go db.connectionOpener(ctx)
	//go db.connectionResetter(ctx)

	return datasource
}
