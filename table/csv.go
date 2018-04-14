package table

import (
	"encoding/csv"
	"github.com/Mitu217/tamate/table/schema"
	"io"
)

// CSVDataSource :
type CSVTable struct {
	schema *schema.Schema
	rows   *Rows
}

func NewCSV(sc *schema.Schema, r io.ReadSeeker) (*CSVTable, error) {
	csvr := csv.NewReader(r)
	rows, err := readRows(csvr)
	if err != nil {
		return nil, err
	}
	tbl := &CSVTable{
		schema: sc,
		rows:   rows,
	}
	return tbl, nil
}

func readRows(csv *csv.Reader) (*Rows, error) {
	rows, err := csv.ReadAll()
	if err != nil {
		return nil, err
	}
	rowCount := len(rows)
	values := make([][]string, rowCount, rowCount)
	for i, row := range rows {
		values[i] = row[1:]
	}
	return &Rows{
		Values: values,
	}, nil
}

// GetSchema :
func (tbl *CSVTable) GetSchema() (*schema.Schema, error) {
	return tbl.schema, nil
}

// GetRows :
func (tbl *CSVTable) GetRows() (*Rows, error) {
	return tbl.rows, nil
}
