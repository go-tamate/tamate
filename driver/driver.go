package driver

import (
	"context"
)

// RowsNextResultSet extends the Rows interface by providing a way to signal
// the driver to advance to the next result set.
type RowsNextResultSet interface {
	Rows

	HasNextResultSet() bool

	NextResultSet() error
}

type Value interface{}

// NamedValue ...
type NamedValue struct {
	Name    string
	Ordinal int
	Value   Value
}

// Rows ...
type Rows interface {
	Columns() []string
	Close() error
	Next(dest []NamedValue) error
}

// Schema ...
type Schema interface{}

// Driver ...
type Driver interface {
	Open(context.Context, string) (DriverContext, error)
}

// DriverContext
type DriverContext interface {
	GetSchema(ctx context.Context, tableName string) (Schema, error)
	GetRows(context.Context, string) (Rows, error)
	Close() error
}
