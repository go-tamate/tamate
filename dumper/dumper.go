package dumper

import (
	"github.com/Mitu217/tamate/datasource"
)

// Dumper :
type Dumper struct{}

// NewDumper :
func NewDumper() *Dumper {
	return &Dumper{}
}

// Dump :
func (d *Dumper) Dump(src datasource.DataSource, dst datasource.DataSource) error {
	dst.SetRows(src.GetRows())
	return nil
}

// DumpRows :
func (d *Dumper) DumpRows(src datasource.DataSource) (*datasource.Rows, error) {
	rows := src.GetRows()
	return rows, nil
}
