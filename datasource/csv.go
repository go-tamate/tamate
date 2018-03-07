package datasource

import (
	"io"
	"errors"
)

func NewCSVDataSource(r io.Reader) (*CSVDataSource, error) {
	// TODO: implements
	return nil, errors.New("not implemented")
}

// CSV data source
type CSVDataSource struct {
}
