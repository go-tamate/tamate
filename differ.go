package tamate

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

func NewDiffer(opts ...Option) (*Differ, error) {
	colDiffer, err := newColumnDiffer()
	if err != nil {
		return nil, err
	}
	rowDiffer, err := newRowDiffer()
	if err != nil {
		return nil, err
	}
	d := &Differ{
		column: colDiffer,
		row:    rowDiffer,
	}

	for _, opt := range opts {
		opt(d)
	}
	return d, nil
}

func (d *Differ) setIgnoreColumnName(name string) error {
	d.column.setIgnoreColumnName(name)
	d.row.setIgnoreColumnName(name)
	return nil
}

func (d *Differ) Diff(schema1, schema2 *Schema, rows1, rows2 []*Row) (*Diff, error) {
	dcols, err := d.DiffColumns(schema1, schema2)
	if err != nil {
		return nil, err
	}
	drows, err := d.DiffRows(schema1, rows1, rows2)
	if err != nil {
		return nil, err
	}
	return &Diff{
		DiffColumns: dcols,
		DiffRows:    drows,
	}, nil
}

func (d *Differ) DiffColumns(schema1, schema2 *Schema) (*DiffColumns, error) {
	return d.column.diff(schema1, schema2)
}

func (d *Differ) DiffRows(schema1 *Schema, rows1, rows2 []*Row) (*DiffRows, error) {
	return d.row.diff(schema1, rows1, rows2)
}
