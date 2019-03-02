package tamate

import (
	"context"
	"testing"

	"github.com/Mitu217/tamate/driver"
	"github.com/stretchr/testify/assert"
)

type fakeDriver struct{}

func (d *fakeDriver) Open(ctx context.Context, dsn string) (driver.Conn, error) {
	return nil, nil
}

func TestRegister(t *testing.T) {
	Register("Register", &fakeDriver{})
}

func TestGetDataSource(t *testing.T) {
	driver := &fakeDriver{}
	Register("GetDataSource", driver)

	_, err := Open("GetDataSource", "")
	assert.NoError(t, err)
}
