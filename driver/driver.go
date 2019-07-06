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

// ColumnValue ...
type ColumnValue struct {
	ColumnName string
	ColumnType ColumnType
	Value      interface{}
}

// Rows ...
type Rows interface {
	Columns() []string
	Close() error
	Next(dest []ColumnValue) error
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
