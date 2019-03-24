package tamate

import (
	"context"
	"fmt"
	"testing"

	"github.com/go-tamate/tamate/driver"
	"github.com/stretchr/testify/assert"
)

type fakeRows struct {
	max            int
	current        int
	fakeColumns    []*driver.Column
	fakeRowsValues [][]driver.Value
}

func (rs *fakeRows) Columns() []*driver.Column {
	return rs.fakeColumns
}

func (rs *fakeRows) Close() error {
	return nil
}

func (rs *fakeRows) Next(dest []driver.Value) error {
	rs.current++
	if rs.current >= rs.max {
		return fmt.Errorf("current is larger than max")
	}
	for i := range dest {
		dest[i] = rs.fakeRowsValues[rs.current][i]
	}
	return nil
}

type fakeConn struct {
	fakeSchema *driver.Schema
	fakeRows   *fakeRows
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

func (c *fakeConn) GetRows(ctx context.Context, name string) (driver.Rows, error) {
	if c.fakeErr != nil {
		return nil, c.fakeErr
	}
	return c.fakeRows, nil
}

func (c *fakeConn) SetRows(ctx context.Context, name string, rowsValues [][]driver.Value) error {
	if c.fakeErr != nil {
		return c.fakeErr
	}
	c.fakeRows.fakeRowsValues = rowsValues
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

		rowsValues = [][]driver.Value{
			[]driver.Value{1, "hana", 16},
			[]driver.Value{2, "tamate", 15},
			[]driver.Value{3, "kamuri", 15},
			[]driver.Value{4, "eiko", 15},
		}
	)

	fakeRows := &fakeRows{
		max:            len(rowsValues),
		current:        -1,
		fakeRowsValues: rowsValues,
		fakeColumns: []*driver.Column{
			driver.NewColumn("id", 0, driver.ColumnTypeInt, false, false),
			driver.NewColumn("name", 1, driver.ColumnTypeString, false, false),
			driver.NewColumn("age", 2, driver.ColumnTypeInt, false, false),
		},
	}
	fakeConn := &fakeConn{
		fakeRows: fakeRows,
	}
	fakeDriver := &fakeDriver{
		fakeConn: fakeConn,
	}

	Register(driverName, fakeDriver)
	ds, err := Open(driverName, dsn)
	defer func() {
		cerr := ds.Close()
		assert.NoError(t, cerr)
	}()
	if assert.NoError(t, err) {
		rows, err := ds.GetRows(ctx, schemaName)
		defer rows.Close()
		if assert.NoError(t, err) {
			res := make([][]driver.Value, 0)
			for rows.Next() {
				rowVals, err := rows.GetRow()
				if assert.NoError(t, err) {
					res = append(res, rowVals)
				}
			}
			assert.Equal(t, rowsValues, res)
		}
	}
}

func TestSetRows(t *testing.T) {
	var (
		ctx        = context.Background()
		driverName = "SetRows"
		schemaName = "Test"
		dsn        = ""

		rowsValues = [][]driver.Value{
			[]driver.Value{1, "hana", 16},
			[]driver.Value{2, "tamate", 15},
			[]driver.Value{3, "kamuri", 15},
			[]driver.Value{4, "eiko", 15},
		}
	)

	fakeRows := &fakeRows{
		fakeRowsValues: [][]driver.Value{},
	}
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
		err := ds.SetRows(ctx, schemaName, rowsValues)
		if assert.NoError(t, err) {
			//assert.EqualValues(t, settingRowsValues, fakeConn.fakeRows)
		}
	}
}
