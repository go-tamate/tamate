package differ

import (
	"errors"

	"fmt"

	"strings"

	"sort"

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
	comparatorMap map[datasource.ColumnType]ValueComparator
}

func createDefaultComparatorMap() map[datasource.ColumnType]ValueComparator {
	cm := make(map[datasource.ColumnType]ValueComparator)
	cm[datasource.ColumnTypeDatetime] = &datetimeComparator{}
	cm[datasource.ColumnTypeBool] = &boolComparator{}
	cm[datasource.ColumnTypeBytes] = &bytesComparator{}

	cm[datasource.ColumnTypeString] = &asStringComparator{}
	cm[datasource.ColumnTypeInt] = &asStringComparator{}
	cm[datasource.ColumnTypeFloat] = &asStringComparator{}
	cm[datasource.ColumnTypeDate] = &asStringComparator{}
	return cm
}

// NewDiffer is create differ instance method
func NewDiffer() (*Differ, error) {
	d := &Differ{comparatorMap: createDefaultComparatorMap()}
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

	pk := sc.PrimaryKey
	if sc.PrimaryKey == nil {
		return nil, errors.New("Primary key required.")
	}

	lmap, err := rowsToPKMap(pk, leftRows)
	if err != nil {
		return nil, err
	}
	rmap, err := rowsToPKMap(pk, rightRows)
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
				same, err := d.isSameRow(sc, lrow, rrow)
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

func rowsToPKMap(pk *datasource.Key, rows []*datasource.Row) (map[string]*datasource.Row, error) {
	rowMap := make(map[string]*datasource.Row, len(rows))
	for _, row := range rows {
		values, ok := row.GroupByKey[pk]
		if !ok {
			return nil, fmt.Errorf("leftRows has no PK(%s) value", pk.String())
		}
		var strvals []string
		for _, v := range values {
			strvals = append(strvals, v.StringValue())
		}
		sort.Strings(strvals)
		pkValue := strings.Join(strvals, "_")
		rowMap[pkValue] = row
	}
	return rowMap, nil
}

func (d *Differ) isSameRow(sc *datasource.Schema, left, right *datasource.Row) (bool, error) {
	colMap := make(map[string]*datasource.Column, len(sc.Columns))
	for _, col := range sc.Columns {
		colMap[col.Name] = col
	}

	for cn, lval := range left.Values {
		rval, rhas := right.Values[cn]
		// そもそも片方に存在しない column であれば、絶対一致しないので即 false
		if !rhas {
			return false, nil
		}
		equal, err := d.valueEqual(colMap[cn], lval, rval)
		if err != nil {
			return false, err
		}
		if !equal {
			return false, nil
		}
	}
	return true, nil
}

func (d *Differ) valueEqual(col *datasource.Column, lv, rv *datasource.GenericColumnValue) (bool, error) {
	cmp, has := d.comparatorMap[col.Type]
	if has {
		return cmp.Equal(col, lv, rv)
	}

	return lv.Value == rv.Value, nil
}
