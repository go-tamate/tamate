package csv

import (
	"context"

	"github.com/Mitu217/tamate"
	"github.com/Mitu217/tamate/driver"
)

const driverName = "csv"

type csvDriver struct{}

func (ds *csvDriver) Open(ctx context.Context, dsn string) (driver.Conn, error) {
	cc := &csvConn{
		rootPath:       dsn,
		columnRowIndex: 0, // TODO: Get from dsn
	}
	return cc, nil
}

func init() {
	tamate.Register(driverName, &csvDriver{})
}
