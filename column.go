package tamate

type DiffColumns struct {
	Left  []*Column
	Right []*Column
}

func (dc *DiffColumns) HasDiff() bool {
	return len(dc.Left) > 0 || len(dc.Right) > 0
}

type columnDiffer struct {
	ignoreColumnNames []string
}

func newColumnDiffer() (*columnDiffer, error) {
	return &columnDiffer{
		ignoreColumnNames: make([]string, 0),
	}, nil
}

func (cd *columnDiffer) setIgnoreColumnName(columnName string) {
	cd.ignoreColumnNames = append(cd.ignoreColumnNames, columnName)
}

func (cd *columnDiffer) shouldIgnore(columnName string) bool {
	for _, ignoreColName := range cd.ignoreColumnNames {
		if ignoreColName == columnName {
			return true
		}
	}
	return false
}

func (cd *columnDiffer) diff(left, right *Schema) (*DiffColumns, error) {
	lmap, err := columnsToNameMap(left.Columns)
	if err != nil {
		return nil, err
	}
	rmap, err := columnsToNameMap(right.Columns)
	if err != nil {
		return nil, err
	}

	diff := &DiffColumns{
		Left:  make([]*Column, 0),
		Right: make([]*Column, 0),
	}
	ldiff := &diff.Left
	rdiff := &diff.Right
	for i := 0; i < 2; i++ {
		for lcn, lcol := range lmap {
			if cd.shouldIgnore(lcn) {
				continue
			}
			rcol, rhas := rmap[lcn]
			if !rhas {
				*ldiff = append(*ldiff, lcol)
				continue
			}
			if i == 0 && !isSameColumn(lcol, rcol) {
				*ldiff = append(*ldiff, lcol)
				*rdiff = append(*rdiff, rcol)
			}
		}
		// swap ref to (left/right)
		lmap, rmap = rmap, lmap
		ldiff, rdiff = rdiff, ldiff
	}
	return diff, nil
}

func columnsToNameMap(cols []*Column) (map[string]*Column, error) {
	colMap := make(map[string]*Column, len(cols))
	for _, col := range cols {
		colMap[col.Name] = col
	}
	return colMap, nil
}

func isSameColumn(left, right *Column) bool {
	return left.Name == right.Name &&
		left.Type == right.Type &&
		left.NotNull == right.NotNull &&
		left.AutoIncrement == right.AutoIncrement
}
