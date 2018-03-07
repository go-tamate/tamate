package datasource

import (
	"encoding/csv"
	"io"
	"os"
)

func NewCSVDataSource(r io.Reader) (*CSVDataSource, error) {
	recodes, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, err
	}
	values := make([][]interface{}, len(recodes))
	for i, recode := range recodes {
		value := make([]interface{}, len(recode))
		for k, v := range recode {
			value[k] = v
		}
		values[i] = value
	}
	ds := &CSVDataSource{
		Values: values,
	}
	return ds, err
}

func NewCSVFileDataSource(path string) (*CSVDataSource, error) {
	r, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return NewCSVDataSource(r)
}

// CSV data source
type CSVDataSource struct {
	Values [][]interface{}
}
