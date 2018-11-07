package differ

import (
	"fmt"
	"sort"
	"strings"

	"github.com/Mitu217/tamate/datasource"
)

func createDefaultComparatorMap() map[datasource.ColumnType]ValueComparator {
	cm := make(map[datasource.ColumnType]ValueComparator)
	cm[datasource.ColumnTypeDatetime] = &datetimeComparator{}
	cm[datasource.ColumnTypeBool] = &boolComparator{}
	cm[datasource.ColumnTypeBytes] = &bytesComparator{}

	cm[datasource.ColumnTypeString] = &asStringComparator{}
	cm[datasource.ColumnTypeInt] = &asStringComparator{}
	cm[datasource.ColumnTypeFloat] = &asStringComparator{}
	cm[datasource.ColumnTypeDate] = &asStringComparator{}

	// TODO: Implement type optimized comparator
	cm[datasource.ColumnTypeStringArray] = &asStringComparator{}
	cm[datasource.ColumnTypeBytesArray] = &asStringComparator{}
	cm[datasource.ColumnTypeFloatArray] = &asStringComparator{}
	cm[datasource.ColumnTypeIntArray] = &asStringComparator{}
	cm[datasource.ColumnTypeDateArray] = &asStringComparator{}
	cm[datasource.ColumnTypeDatetimeArray] = &asStringComparator{}
	cm[datasource.ColumnTypeBoolArray] = &asStringComparator{}

	return cm
}

type DiffRows struct {
	Left  []*datasource.Row `json:"left"`
	Right []*datasource.Row `json:"right"`
}

func (dr *DiffRows) HasDiff() bool {
	return len(dr.Left) > 0 || len(dr.Right) > 0
}

type rowDiffer struct {
	comparatorMap     map[datasource.ColumnType]ValueComparator
	ignoreColumnNames []string
}

func newRowDiffer() (*rowDiffer, error) {
	return &rowDiffer{
		comparatorMap:     createDefaultComparatorMap(),
		ignoreColumnNames: make([]string, 0),
	}, nil
}

func (rd *rowDiffer) setIgnoreColumnName(columnName string) {
	rd.ignoreColumnNames = append(rd.ignoreColumnNames, columnName)
}

func (rd *rowDiffer) shouldIgnore(columnName string) bool {
	for _, ignoreColName := range rd.ignoreColumnNames {
		if ignoreColName == columnName {
			return true
		}
	}
	return false
}

func (rd *rowDiffer) diff(schema *datasource.Schema, leftRows, rightRows []*datasource.Row) (*DiffRows, error) {
	lmap, err := RowsToPrimaryKeyMap(schema, leftRows)
	if err != nil {
		return nil, err
	}
	rmap, err := RowsToPrimaryKeyMap(schema, rightRows)
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
				same, err := rd.IsSameRow(lrow, rrow)
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

func (rd *rowDiffer) IsSameRow(left, right *datasource.Row) (bool, error) {
	for cn, lval := range left.Values {
		if rd.shouldIgnore(cn) {
			continue
		}
		rval, rhas := right.Values[cn]
		if !rhas {
			return false, nil
		}
		if equal, err := rd.ValueEqual(lval, rval); !equal || err != nil {
			return false, err
		}
	}
	return true, nil
}

func (rd *rowDiffer) ValueEqual(lv, rv *datasource.GenericColumnValue) (bool, error) {
	if cmp, has := rd.comparatorMap[lv.Column.Type]; has {
		return cmp.Equal(lv.Column, lv, rv)
	}
	return lv.Value == rv.Value, nil
}
