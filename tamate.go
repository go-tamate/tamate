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

// Error ...
func (e *tamateError) Error() string {
	return fmt.Sprintf("tamate.%s: %s", e.Func, e.Err.Error())
}

func registerNilDriverError(fn string) *tamateError {
	return &tamateError{
		Func: fn,
		Err:  errors.New("driver is nil"),
	}
}

func registerDuplicatedNameError(fn, name string) *tamateError {
	return &tamateError{
		Func: fn,
		Err:  fmt.Errorf("called twice for driver %s", name),
	}
}

func notRegisterDriverErr(fn, driverName string) *tamateError {
	return &tamateError{
		Func: fn,
		Err:  fmt.Errorf("tamate: unknown driver %q (forgotten import?)", driverName),
	}
}

func notCalledNextError(fn string) *tamateError {
	return &tamateError{
		Func: fn,
		Err:  errors.New("called without called Next()"),
	}
}

func alreadyClosedError(fn string) *tamateError {
	return &tamateError{
		Func: fn,
		Err:  errors.New("already closed"),
	}
}

func notEqualColumnLengthError(fn string, exp, act int) *tamateError {
	return &tamateError{
		Func: fn,
		Err:  fmt.Errorf("tamate: expected %d destination columns, not %d", exp, act),
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

// Rows ...
type Rows struct {
	rowsi    driver.Rows
	lastcols []*driver.NamedValue

	// closemu guards lasterr and closed.
	closeMu sync.RWMutex
	closed  bool
	lasterr error
}

// Next ...
func (rs *Rows) Next() bool {
	doClose, ok := rs.nextLocked()
	if doClose {
		rs.Close()
	}
	return ok
}

func (rs *Rows) nextLocked() (doClose, ok bool) {
	if rs.closed {
		return false, false
	}

	rs.closeMu.RLock()
	defer rs.closeMu.RUnlock()

	if rs.lastcols == nil {
		rs.lastcols = make([]*driver.NamedValue, len(rs.rowsi.Columns()))
	}

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
	return false, true
}

// Close ...
func (rs *Rows) Close() error {
	rs.closeMu.Lock()
	defer rs.closeMu.Unlock()
	if rs.closed {
		return nil
	}
	rs.closed = true
	return rs.rowsi.Close()
}

// Scan ...
func (rs *Rows) Scan(dest []*driver.NamedValue) error {
	const fnName = "Scan"

	rs.closeMu.RLock()
	defer rs.closeMu.RUnlock()

	if rs.lasterr != nil && rs.lasterr != io.EOF {
		return rs.lasterr
	}
	if rs.closed {
		return alreadyClosedError(fnName)
	}
	if rs.lastcols == nil {
		return notCalledNextError(fnName)
	}
	if len(rs.rowsi.Columns()) != len(rs.lastcols) {
		return notEqualColumnLengthError(fnName, len(rs.lastcols), len(rs.rowsi.Columns()))
	}
	dest = make([]*driver.NamedValue, len(rs.lastcols))
	for i, sc := range rs.lastcols {
		dest[i] = sc
	}
	return nil
}

// Err ...
func (rs *Rows) Err() error {
	rs.closeMu.RLock()
	defer rs.closeMu.RUnlock()
	return rs.lasterr
}

// DataSource ...
type DataSource struct {
	ctx driver.DriverContext

	// protected closed
	closeMu sync.RWMutex
	closed  bool
}

// OpenDataSource
func OpenDataSource(ctx driver.DriverContext) *DataSource {
	return &DataSource{
		ctx: ctx,

		closed: false,
	}
}

// GetSchema ...
func (ds *DataSource) GetSchema(ctx context.Context, name string) (driver.Schema, error) {
	const fnName = "GetSchema"

	ds.closeMu.RLock()
	defer ds.closeMu.RUnlock()

	if ds.closed {
		return nil, alreadyClosedError(fnName)
	}

	return ds.ctx.GetSchema(ctx, name)
}

// GetRows ...
func (ds *DataSource) GetRows(ctx context.Context, name string) (*Rows, error) {
	const fnName = "GetRows"

	ds.closeMu.RLock()
	defer ds.closeMu.RUnlock()

	if ds.closed {
		return nil, alreadyClosedError(fnName)
	}

	rows, err := ds.ctx.GetRows(ctx, name)
	if err != nil {
		return nil, err
	}
	return &Rows{
		rowsi: rows,
	}, nil
}

// Close ...
func (ds *DataSource) Close() error {
	ds.closeMu.Lock()
	defer ds.closeMu.Unlock()

	if ds.closed {
		return nil
	}
	ds.closed = true
	return ds.ctx.Close()
}

// Open ...
func Open(driverName, dataSourceName string) (*DataSource, error) {
	const fnName = "Open"

	driversMu.RLock()
	defer driversMu.RUnlock()

	driveri, ok := drivers[driverName]
	if !ok {
		return nil, notRegisterDriverErr(fnName, driverName)
	}
	driverCtx, err := driveri.Open(context.Background(), dataSourceName)
	if err != nil {
		return nil, err
	}
	return OpenDataSource(driverCtx), nil
}
