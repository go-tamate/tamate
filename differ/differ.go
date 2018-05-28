package differ

import (
	"errors"

	"fmt"
	"github.com/Mitu217/tamate/datasource"
)

// DiffColumns is add, modify and delete columns struct
type DiffColumns struct {
	Left  []*datasource.Column `json:"left"`
	Right []*datasource.Column `json:"right"`
}

// DiffRows is modify row values struct between left and right
type DiffRows struct {
	Left  []*datasource.Row `json:"left"`
	Right []*datasource.Row `json:"right"`
}

// Differ is diff between tables struct
type Differ struct {
}

// NewDiffer is create differ instance method
func NewDiffer() (*Differ, error) {
	d := &Differ{}
	return d, nil
}

// DiffColumns is get diff columns method
func (d *Differ) DiffColumns(left, right *datasource.Schema) (*DiffColumns, error) {
	lmap, err := columnsToNameMap(left.Columns)
	if err != nil {
		return nil, err
	}
	rmap, err := columnsToNameMap(right.Columns)
	if err != nil {
		return nil, err
	}

	diff := &DiffColumns{}
	ldiff := &diff.Left
	rdiff := &diff.Right
	for i := 0; i < 2; i++ {
		for lcn, lcol := range lmap {
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
	}
	// swap ref to (left/right)
	lmap, rmap = rmap, lmap
	ldiff, rdiff = rdiff, ldiff
	return diff, nil
}

func columnsToNameMap(cols []*datasource.Column) (map[string]*datasource.Column, error) {
	colMap := make(map[string]*datasource.Column, len(cols))
	for _, col := range cols {
		colMap[col.Name] = col
	}
	return colMap, nil
}

func isSameColumn(left, right *datasource.Column) bool {
	return left.Name == right.Name &&
		left.Type == right.Type &&
		left.NotNull == right.NotNull &&
		left.AutoIncrement == right.AutoIncrement
}

// DiffRows is get diff rows method
func (d *Differ) DiffRows(sc *datasource.Schema, leftRows, rightRows []*datasource.Row) (*DiffRows, error) {
	if sc.PrimaryKey == nil {
		return nil, errors.New("Primary key required.")
	}
	pkn := sc.PrimaryKey.ColumnNames[0]

	lmap, err := rowsToPKMap(pkn, leftRows)
	if err != nil {
		return nil, err
	}
	rmap, err := rowsToPKMap(pkn, rightRows)
	if err != nil {
		return nil, err
	}

	diff := &DiffRows{}
	ldiff := &diff.Left
	rdiff := &diff.Right
	for i := 0; i < 2; i++ {
		for pkv, lrow := range lmap {
			rrow, rhas := rmap[pkv]
			if !rhas {
				*ldiff = append(*ldiff, lrow)
				continue
			}

			// 一致する pk がある場合の差分チェックは1回しか行わない（normal, reverse で2回しないようにする
			if i == 0 {
				same, err := isSameRow(sc, lrow, rrow)
				if err != nil {
					return nil, err
				}
				if !same {
					*ldiff = append(*ldiff, lrow)
					*rdiff = append(*rdiff, rrow)
				}
			}
		}
		// swap ref to (left/right)
		lmap, rmap = rmap, lmap
		ldiff, rdiff = rdiff, ldiff
	}
	return diff, nil
}

func rowsToPKMap(pkName string, rows []*datasource.Row) (map[string]*datasource.Row, error) {
	rowMap := make(map[string]*datasource.Row, len(rows))
	for _, row := range rows {
		pkv, ok := row.Values[pkName]
		if !ok {
			return nil, fmt.Errorf("leftRows has no PK(%s) value", pkName)
		}
		rowMap[pkv.StringValue()] = row
	}
	return rowMap, nil
}

func isSameRow(sc *datasource.Schema, left, right *datasource.Row) (bool, error) {
	colMap := make(map[string]*datasource.Column, len(sc.Columns))
	for _, col := range sc.Columns {
		colMap[col.Name] = col
	}

	for cn, lval := range left.Values {
		rval, rhas := right.Values[cn]
		// TODO: implements comparator
		// そもそも片方に存在しない行なのであれば、絶対一致しないので即 false
		if !rhas {
			return false, nil
		}
		same, err := compareValues(colMap[cn], lval, rval)
		if err != nil {
			return false, err
		}
		if !same {
			return false, nil
		}
	}
	return true, nil
}

func compareValues(col *datasource.Column, lv, rv *datasource.GenericColumnValue) (bool, error) {
	if col.Type == datasource.ColumnTypeDatetime {
		ltv, err := lv.TimeValue()
		if err != nil {
			return false, err
		}
		rtv, err := rv.TimeValue()
		if err != nil {
			return false, err
		}
		return ltv == rtv, nil
	}

	// TODO: more comparator
	return lv.Value == rv.Value, nil
}
