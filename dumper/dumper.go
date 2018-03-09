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
	columns := src.GetColumns()
	values := src.GetValues()
	dst.SetColumns(columns)
	dst.SetValues(values)

	return nil
}

// DumpRows :
func (d *Dumper) DumpRows(src datasource.DataSource) (*Rows, error) {
	columns := src.GetColumns()
	values := src.GetValues()

	rows := &Rows{
		Columns: columns,
		Values:  values,
	}
	return rows, nil
}
