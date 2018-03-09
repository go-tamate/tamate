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
	rows, err := src.GetRows()
	if err != nil {
		return err
	}
	if err := dst.SetRows(rows); err != nil {
		return err
	}
	return nil
}
