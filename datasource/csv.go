package datasource

import (
	"io"
	"github.com/pkg/errors"
)

// CSV data source
type CSVDataSource struct {
}

func (ds *CSVDataSource) Output(dst DataSource, w io.Writer) error {
	// TODO: implements
	return errors.New("not implemented")
}