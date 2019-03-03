package driver

import "context"

type Driver interface {
	Open(context.Context, string) (Conn, error)
}

type Conn interface {
	GetSchema(context.Context, string) (*Schema, error)
	SetSchema(context.Context, *Schema) error
	GetRows(context.Context, *Schema) ([]*Row, error)
	SetRows(context.Context, *Schema, []*Row) error

	Close() error
}

type Connector interface {
	Connect(context.Context) (Conn, error)
	Driver() Driver
}
