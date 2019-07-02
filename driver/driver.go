package driver

import (
	"context"
)

// RowsNextResultSet extends the Rows interface by providing a way to signal
// the driver to advance to the next result set.
type RowsNextResultSet interface {
	Rows

	// HasNextResultSet is called at the end of the current result set and
	// reports whether there is another result set after the current one.
	HasNextResultSet() bool

	// NextResultSet advances the driver to the next result set even
	// if there are remaining rows in the current result set.
	//
	// NextResultSet should return io.EOF when there are no more result sets.
	NextResultSet() error
}

type Value interface{}

type Rows interface {
	Columns() []*Column
	Close() error
	Next(dest []Value) error
}

type Driver interface {
	Open(context.Context, string) (Conn, error)
}

type Conn interface {
	GetSchema(context.Context, string) (*Schema, error)
	SetSchema(context.Context, string, *Schema) error
	GetRows(context.Context, string) (Rows, error)
	SetRows(context.Context, string, [][]Value) error

	Close() error
}

type Connector interface {
	Connect(context.Context) (Conn, error)
	Driver() Driver
}
