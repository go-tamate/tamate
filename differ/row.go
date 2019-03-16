package differ

import (
	"fmt"
	"sort"
	"strings"

	"github.com/go-tamate/tamate/driver"
)

type DiffRows struct {
	Left  []*driver.Row
	Right []*driver.Row
}

func (dr *DiffRows) HasDiff() bool {
	return len(dr.Left) > 0 || len(dr.Right) > 0
}

type rowDiffer struct {
	comparatorMap     ComparatorMap
	ignoreColumnNames []string
}

func newRowDiffer() *rowDiffer {
	return &rowDiffer{
		comparatorMap:     NewComparatorMap(),
		ignoreColumnNames: make([]string, 0),
	}
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

func (rd *rowDiffer) diff(schema *driver.Schema, leftRows, rightRows []*driver.Row) (*DiffRows, error) {
	lmap, err := RowsToPrimaryKeyMap(schema, leftRows)
	if err != nil {
		return nil, err
	}
	rmap, err := RowsToPrimaryKeyMap(schema, rightRows)
	if err != nil {
		return nil, err
	}

	diff := &DiffRows{
		Left:  make([]*driver.Row, 0),
		Right: make([]*driver.Row, 0),
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

func (rd *rowDiffer) IsSameRow(left, right *driver.Row) (bool, error) {
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

func (rd *rowDiffer) ValueEqual(lval, rval *driver.GenericColumnValue) (bool, error) {
	return rd.comparatorMap.Equal(lval, rval)
}

func RowsToPrimaryKeyMap(schema *driver.Schema, rows []*driver.Row) (map[string]*driver.Row, error) {
	primaryKeyString := schema.PrimaryKey.String()
	primaryKeyMap := make(map[string]*driver.Row, len(rows))
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
		resRow := &driver.Row{
			GroupByKey: row.GroupByKey,
			Values:     make(driver.RowValues),
		}
		for _, column := range schema.Columns {
			for rowColumnName, columnValues := range row.Values {
				if rowColumnName == column.Name {
					resRow.Values[column.Name] = columnValues
					break
				}
			}
			if _, has := resRow.Values[column.Name]; !has {
				resRow.Values[column.Name] = driver.NewGenericColumnValue(column, "")
			}
		}
		primaryKeyMap[k] = resRow
	}
	return primaryKeyMap, nil
}
