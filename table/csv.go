package table

import (
	"encoding/csv"
	"github.com/Mitu217/tamate/table/schema"
	"github.com/pkg/errors"
	"io"
	"os"
)

type CSVConfig struct {
	Path string `json:"path"`
}

// CSVDataSource :
type CSVTable struct {
	Schema *schema.Schema `json:"schema"`
	Config *CSVConfig     `json:"config"`
	rows   *Rows
}

func NewCSV(sc *schema.Schema, conf *CSVConfig) (*CSVTable, error) {
	tbl := &CSVTable{
		Schema: sc,
		Config: conf,
	}
	return tbl, nil
}

// for test
func newCSVFromReader(sc *schema.Schema, r io.Reader) (*CSVTable, error) {
	rows, err := readRows(csv.NewReader(r))
	if err != nil {
		return nil, err
	}
	tbl := &CSVTable{
		Schema: sc,
		rows:   rows,
	}
	return tbl, nil
}

func readRowsFromFile(filename string) (*Rows, error) {
	r, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer r.Close()
	return readRows(csv.NewReader(r))
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
	return tbl.Schema, nil
}

// GetRows :
func (tbl *CSVTable) GetRows() (*Rows, error) {
	if tbl.rows == nil {
		if tbl.Config == nil {
			return nil, errors.New("no csv config")
		}
		rows, err := readRowsFromFile(tbl.Config.Path)
		if err != nil {
			return nil, err
		}
		tbl.rows = rows
	}
	return tbl.rows, nil
}
