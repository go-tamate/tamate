package tamate

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/go-tamate/tamate/driver"
)

var (
	driversMu sync.RWMutex
	drivers   = make(map[string]driver.Driver)
)

type tamateError struct {
	Func string
	Err  error
}

func (e *tamateError) Error() string {
	return fmt.Sprintf("tamate.%s: %s", e.Func, e.Err.Error())
}

func registerNilDriverError(fn string) *tamateError {
	return &tamateError{
		Func: fn,
		Err:  errors.New("driver is nil"),
	}
}

func registerDuplicatedNameError(fn string, name string) *tamateError {
	return &tamateError{
		Func: fn,
		Err:  fmt.Errorf("called twice for driver %s", name),
	}
}

// Register makes tamate driver available by the provided name.
// If Register is called twice with the same name or if driver is nil, it panic.
func Register(name string, driver driver.Driver) {
	const funcName = "Register"

	driversMu.Lock()
	defer driversMu.Unlock()
	if driver == nil {
		panic(registerNilDriverError(funcName).Error())
	}
	if _, dup := drivers[name]; dup {
		panic(registerDuplicatedNameError(funcName, name).Error())
	}
	drivers[name] = driver
}

// For test.
func unregisterAllDrivers() {
	driversMu.Lock()
	defer driversMu.Unlock()
	drivers = make(map[string]driver.Driver)
}

// Drivers returns a sorted list of the names of the registered drivers.
func Drivers() []string {
	driversMu.RLock()
	defer driversMu.RUnlock()
	var list []string
	for name := range drivers {
		list = append(list, name)
	}
	sort.Strings(list)
	return list
}

type Rows struct {
	rowsi    driver.Rows
	lastcols []driver.Value

	// closemu guards lasterr and closed.
	closeMu sync.RWMutex
	closed  bool
	lasterr error
}

func (rs *Rows) Next() bool {
	doClose, ok := rs.nextLocked()
	if doClose {
		rs.Close()
	}
	return ok
}

func (rs *Rows) GetRow() ([]driver.Value, error) {
	if rs.lastcols == nil {
		return nil, errors.New("tamate: GetRow called without calling Next")
func (rs *Rows) nextLocked() (doClose, ok bool) {
	if rs.closed {
		return false, false
	}

	dest := make([]driver.Value, len(rs.lastcols))
	for i := range rs.lastcols {
		if err := convertAssign(&dest[i], rs.lastcols[i]); err != nil {
			return nil, err
		}
	}
	return dest, nil
}

type DataSource struct {
	connector  driver.Connector
	driverConn driver.Conn
	stop       func()
}
	rs.closeMu.RLock()
	defer rs.closeMu.RUnlock()

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
	if rs.lastcols == nil {
		rs.lastcols = make([]driver.Value, len(rs.rowsi.Columns()))
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

func Open(name string, dsn string) (*DataSource, error) {
	driveri, ok := drivers[name]
	if !ok {
		return nil, fmt.Errorf("tamate: unknown datasource %q (forgotten import?)", name)
	rs.lasterr = rs.rowsi.Next(rs.lastcols)
	if rs.lasterr != nil {
		if rs.lasterr != io.EOF {
			return true, false
		}
		nextResultSet, ok := rs.rowsi.(driver.RowsNextResultSet)
		if !ok {
			return true, false
		}
		if !nextResultSet.HasNextResultSet() {
			doClose = true
		}
		return doClose, false
	}

	return OpenDataSource(&dsnConnector{dsn: dsn, driver: driveri})
	return false, true
}

func OpenDataSource(connector driver.Connector) (*DataSource, error) {
	ctx, cancel := context.WithCancel(context.Background())

	dataSource := &DataSource{
		connector: connector,
		stop:      cancel,
func (rs *Rows) Close() error {
	rs.closeMu.Lock()
	defer rs.closeMu.Unlock()
	if rs.closed {
		return nil
	}

	driverConn, err := connector.Connect(ctx)
	if err != nil {
		return nil, err
	rs.closed = true
	if err := rs.rowsi.Close(); err != nil {
		return err
	}
	dataSource.driverConn = driverConn

	return dataSource, nil
	return nil
}
