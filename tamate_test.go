package tamate

import (
	"context"
	"testing"

	"github.com/go-tamate/tamate/driver"
	"github.com/stretchr/testify/assert"
)

type fakeConn struct {
	fakeSchema *driver.Schema
	fakeRows   []*driver.Row
	fakeErr    error
}

func (c *fakeConn) GetSchema(ctx context.Context, name string) (*driver.Schema, error) {
	if c.fakeErr != nil {
		return nil, c.fakeErr
	}
	return c.fakeSchema, nil
}

func (c *fakeConn) SetSchema(ctx context.Context, name string, schema *driver.Schema) error {
	if c.fakeErr != nil {
		return c.fakeErr
	}
	c.fakeSchema = schema
	return nil
}

func (c *fakeConn) GetRows(ctx context.Context, name string) ([]*driver.Row, error) {
	if c.fakeErr != nil {
		return nil, c.fakeErr
	}
	return c.fakeRows, nil
}

func (c *fakeConn) SetRows(ctx context.Context, name string, rows []*driver.Row) error {
	if c.fakeErr != nil {
		return c.fakeErr
	}
	c.fakeRows = rows
	return nil
}

func (c *fakeConn) Close() error {
	return nil
}

type fakeDriver struct {
	fakeConn *fakeConn
}

func (d *fakeDriver) Open(ctx context.Context, dsn string) (driver.Conn, error) {
	return d.fakeConn, nil
}

func TestRegister(t *testing.T) {
	Register("Register", &fakeDriver{})
}

func TestOpen(t *testing.T) {
	var (
		driverName = "Open"
		dsn        = ""
	)

	driver := &fakeDriver{}
	Register(driverName, driver)

	ds, err := Open(driverName, dsn)
	defer func() {
		cerr := ds.Close()
		assert.NoError(t, cerr)
	}()
	if assert.NoError(t, err) {
		// Check if it matches the registered one
		assert.Equal(t, driver, ds.connector.Driver())
	}
}

func TestGetSchema(t *testing.T) {
	var (
		ctx        = context.Background()
		driverName = "GetSchema"
		schemaName = "Test"
		dsn        = ""
	)

	fakeSchema := &driver.Schema{
		Name: schemaName,
	}
	fakeConn := &fakeConn{
		fakeSchema: fakeSchema,
	}
	driver := &fakeDriver{
		fakeConn: fakeConn,
	}

	Register(driverName, driver)
	ds, err := Open(driverName, dsn)
	defer func() {
		cerr := ds.Close()
		assert.NoError(t, cerr)
	}()
	if assert.NoError(t, err) {
		schema, err := ds.GetSchema(ctx, schemaName)
		if assert.NoError(t, err) {
			assert.EqualValues(t, fakeSchema, schema)
		}
	}
}

func TestSetSchema(t *testing.T) {
	var (
		ctx        = context.Background()
		driverName = "SetSchema"
		schemaName = "Test"
		dsn        = ""
	)

	fakeSchema := &driver.Schema{
		Name: schemaName,
	}
	fakeConn := &fakeConn{}
	driver := &fakeDriver{
		fakeConn: fakeConn,
	}

	Register(driverName, driver)
	ds, err := Open(driverName, dsn)
	defer func() {
		cerr := ds.Close()
		assert.NoError(t, cerr)
	}()
	if assert.NoError(t, err) {
		err := ds.SetSchema(ctx, schemaName, fakeSchema)
		if assert.NoError(t, err) {
			assert.EqualValues(t, fakeSchema, fakeConn.fakeSchema)
		}
	}
}

func TestGetRows(t *testing.T) {
	var (
		ctx        = context.Background()
		driverName = "GetRows"
		schemaName = "Test"
		dsn        = ""
	)

	fakeRows := []*driver.Row{}
	fakeConn := &fakeConn{
		fakeRows: fakeRows,
	}
	driver := &fakeDriver{
		fakeConn: fakeConn,
	}

	Register(driverName, driver)
	ds, err := Open(driverName, dsn)
	defer func() {
		cerr := ds.Close()
		assert.NoError(t, cerr)
	}()
	if assert.NoError(t, err) {
		rows, err := ds.GetRows(ctx, schemaName)
		if assert.NoError(t, err) {
			assert.EqualValues(t, fakeRows, rows)
		}
	}
}

func TestSetRows(t *testing.T) {
	var (
		ctx        = context.Background()
		driverName = "SetRows"
		schemaName = "Test"
		dsn        = ""
	)

	fakeRows := []*driver.Row{}
	fakeConn := &fakeConn{}
	driver := &fakeDriver{
		fakeConn: fakeConn,
	}

	Register(driverName, driver)
	ds, err := Open(driverName, dsn)
	defer func() {
		cerr := ds.Close()
		assert.NoError(t, cerr)
	}()
	if assert.NoError(t, err) {
		err := ds.SetRows(ctx, schemaName, fakeRows)
		if assert.NoError(t, err) {
			assert.EqualValues(t, fakeRows, fakeConn.fakeRows)
		}
	}
}
