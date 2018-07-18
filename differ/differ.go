package differ

import (
	"fmt"
	"sort"
	"strings"

	"github.com/Mitu217/tamate/datasource"
)

type DiffColumns struct {
	Left  []*datasource.Column `json:"left"`
	Right []*datasource.Column `json:"right"`
}

type DiffRows struct {
	Left  []*datasource.Row `json:"left"`
	Right []*datasource.Row `json:"right"`
}

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

	// @todo Implement type optimized comparator
	cm[datasource.ColumnTypeStringArray] = &asStringComparator{}
	cm[datasource.ColumnTypeBytesArray] = &asStringComparator{}
	cm[datasource.ColumnTypeFloatArray] = &asStringComparator{}
	cm[datasource.ColumnTypeIntArray] = &asStringComparator{}
	cm[datasource.ColumnTypeDateArray] = &asStringComparator{}
	cm[datasource.ColumnTypeDatetimeArray] = &asStringComparator{}
	cm[datasource.ColumnTypeBoolArray] = &asStringComparator{}

	return cm
}

func NewDiffer() (*Differ, error) {
	d := &Differ{comparatorMap: createDefaultComparatorMap()}
	return d, nil
}

/*
 * Columns
 */

func (d *Differ) DiffColumns(left, right *datasource.Schema) (*DiffColumns, error) {
	lmap, err := columnsToNameMap(left.Columns)
	if err != nil {
		return nil, err
	}
	rmap, err := columnsToNameMap(right.Columns)
	if err != nil {
		return nil, err
	}

	diff := &DiffColumns{
		Left:  make([]*datasource.Column, 0),
		Right: make([]*datasource.Column, 0),
	}
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
		// swap ref to (left/right)
		lmap, rmap = rmap, lmap
		ldiff, rdiff = rdiff, ldiff
	}
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
		left.OrdinalPosition == right.OrdinalPosition &&
		left.Type == right.Type &&
		left.NotNull == right.NotNull &&
		left.AutoIncrement == right.AutoIncrement
}

/*
 * Rows
 */

func (d *Differ) DiffRows(schema *datasource.Schema, leftRows, rightRows []*datasource.Row) (*DiffRows, error) {
	lmap, err := rowsToPrimaryKeyMap(schema, leftRows)
	if err != nil {
		return nil, err
	}
	rmap, err := rowsToPrimaryKeyMap(schema, rightRows)
	if err != nil {
		return nil, err
	}

	diff := &DiffRows{
		Left:  make([]*datasource.Row, 0),
		Right: make([]*datasource.Row, 0),
	}
	ldiff := &diff.Left
	rdiff := &diff.Right
	for i := 0; i < 2; i++ {
		for pkv, lrow := range lmap {
			rrow, rhas := rmap[pkv]
			if !rhas {
				*ldiff = append(*ldiff, lrow)
				continue
			}
			if i == 0 { // only once
				same, err := d.isSameRow(lrow, rrow)
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

func RowsToPrimaryKeyMap(schema *datasource.Schema, rows []*datasource.Row) (map[string]*datasource.Row, error) {
	primaryKeyString := schema.PrimaryKey.String()
	primaryKeyMap := make(map[string]*datasource.Row, len(rows))
	for _, row := range rows {
		columnValues, ok := row.GroupByKey[primaryKeyString]
		if !ok {
			return nil, fmt.Errorf("rows has no PK(%s) value", primaryKeyString)
		}
		var primaryValues []string
		for _, columnValue := range columnValues {
			primaryValues = append(primaryValues, columnValue.StringValue())
		}
		sort.Strings(primaryValues)
		k := strings.Join(primaryValues, "_")

		// completion column
		resRow := &datasource.Row{
			GroupByKey: row.GroupByKey,
			Values:     make(datasource.RowValues),
		}
		for _, column := range schema.Columns {
			for rowColumnName, columnValues := range row.Values {
				if rowColumnName == column.Name {
					resRow.Values[column.Name] = columnValues
					break
				}
			}
			if _, has := resRow.Values[column.Name]; !has {
				resRow.Values[column.Name] = datasource.NewGenericColumnValue(column)
			}
		}
		primaryKeyMap[k] = resRow
	}
	return primaryKeyMap, nil
}

func (d *Differ) IsSameRow(left, right *datasource.Row) (bool, error) {
	for cn, lval := range left.Values {
		rval, rhas := right.Values[cn]
		if !rhas {
			return false, nil
		}
		if equal, err := d.ValueEqual(lval, rval); !equal || err != nil {
			return false, err
		}
	}
	return true, nil
}

func (d *Differ) ValueEqual(lv, rv *datasource.GenericColumnValue) (bool, error) {
	if cmp, has := d.comparatorMap[lv.Column.Type]; has {
		return cmp.Equal(lv.Column, lv, rv)
	}
	return lv.Value == rv.Value, nil
}
