package differ

import "github.com/go-tamate/tamate/driver"

type Diff struct {
	DiffColumns *DiffColumns
	DiffRows    *DiffRows
}

func (d *Diff) HasDiff() bool {
	return d.DiffColumns.HasDiff() || d.DiffRows.HasDiff()
}

type Differ struct {
	column *columnDiffer
	row    *rowDiffer
}

func NewDiffer(opts ...DifferOption) (*Differ, error) {
	d := &Differ{
		column: newColumnDiffer(),
		row:    newRowDiffer(),
	}
	// set options
	for _, opt := range opts {
		opt(d)
	}
	return d, nil
}

func (d *Differ) Diff(schema1, schema2 *driver.Schema, rows1, rows2 []*driver.Row) (*Diff, error) {
	dcols, err := d.column.diff(schema1, schema2)
	if err != nil {
		return nil, err
	}
	drows, err := d.row.diff(schema1, rows1, rows2)
	if err != nil {
		return nil, err
	}
	return &Diff{
		DiffColumns: dcols,
		DiffRows:    drows,
	}, nil
}

// Deprecated:
func (d *Differ) DiffColumns(schema1, schema2 *driver.Schema) (*DiffColumns, error) {
	return d.column.diff(schema1, schema2)
}

// Deprecated:
func (d *Differ) DiffRows(schema1 *driver.Schema, rows1, rows2 []*driver.Row) (*DiffRows, error) {
	return d.row.diff(schema1, rows1, rows2)
}

type DifferOption func(*Differ) error

func IgnoreColumn(name string) DifferOption {
	return func(d *Differ) error {
		d.column.setIgnoreColumnName(name)
		d.row.setIgnoreColumnName(name)
		return nil
	}
}
