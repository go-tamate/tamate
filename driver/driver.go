package driver

import (
	"context"
)

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
