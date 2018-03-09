package dumper

import (
	"github.com/Mitu217/tamate/datasource"
)

// Dumper :
type Dumper struct{}

// Rows :
type Rows struct {
	Columns []string
	Values  [][]string
}

// NewDumper :
func NewDumper() *Dumper {
	return &Dumper{}
}

// Dump :
func (d *Dumper) Dump(src datasource.DataSource, dst datasource.DataSource) error {
	columns, err := src.GetColumns()
	if err != nil {
		return err
	}
	err = dst.SetColumns(columns)
	if err != nil {
		return err
	}

	values, err := src.GetValues()
	if err != nil {
		return err
	}
	err = dst.SetValues(values)
	if err != nil {
		return err
	}
	return nil
}

// DumpRows :
func (d *Dumper) DumpRows(src datasource.DataSource) (*Rows, error) {
	rows := &Rows{}

	columns, err := src.GetColumns()
	if err != nil {
		return rows, err
	}
	values, err := src.GetValues()
	if err != nil {
		return rows, err
	}

	rows.Columns = columns
	rows.Values = values
	return rows, nil
}
