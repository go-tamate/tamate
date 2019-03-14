package driver

import "context"

type Driver interface {
	Open(context.Context, string) (Conn, error)
}

type Conn interface {
	GetSchema(context.Context, string) (*Schema, error)
	SetSchema(context.Context, string, *Schema) error
	GetRows(context.Context, string) ([]*Row, error)
	SetRows(context.Context, string, []*Row) error

	Close() error
}

type Connector interface {
	Connect(context.Context) (Conn, error)
	Driver() Driver
}
