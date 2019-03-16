package differ

import (
	"bytes"
	"fmt"

	"github.com/go-tamate/tamate/driver"
)

type ComparatorMap map[driver.ColumnType]ValueComparator

func NewComparatorMap() ComparatorMap {
	cm := make(ComparatorMap)

	cm[driver.ColumnTypeDatetime] = &datetimeComparator{}
	cm[driver.ColumnTypeBool] = &boolComparator{}
	cm[driver.ColumnTypeBytes] = &bytesComparator{}

	cm[driver.ColumnTypeString] = &asStringComparator{}
	cm[driver.ColumnTypeInt] = &asStringComparator{}
	cm[driver.ColumnTypeFloat] = &asStringComparator{}
	cm[driver.ColumnTypeDate] = &asStringComparator{}

	// TODO: Implement type optimized comparator
	cm[driver.ColumnTypeStringArray] = &asStringComparator{}
	cm[driver.ColumnTypeBytesArray] = &asStringComparator{}
	cm[driver.ColumnTypeFloatArray] = &asStringComparator{}
	cm[driver.ColumnTypeIntArray] = &asStringComparator{}
	cm[driver.ColumnTypeDateArray] = &asStringComparator{}
	cm[driver.ColumnTypeDatetimeArray] = &asStringComparator{}
	cm[driver.ColumnTypeBoolArray] = &asStringComparator{}

	return cm
}

func (cmap ComparatorMap) Equal(v1, v2 *driver.GenericColumnValue) (bool, error) {
	if cmp, has := cmap[v1.Column.Type]; has {
		return cmp.Equal(v1, v2)
	}
	return v1.Value == v2.Value, nil
}

type ValueComparator interface {
	Equal(v1, v2 *driver.GenericColumnValue) (bool, error)
}

type datetimeComparator struct{}

func (cmp *datetimeComparator) Equal(v1, v2 *driver.GenericColumnValue) (bool, error) {
	return v1.Time() == v2.Time(), nil
}

type asStringComparator struct{}

func (cmp *asStringComparator) Equal(v1, v2 *driver.GenericColumnValue) (bool, error) {
	return v1.String() == v2.String(), nil
}

type boolComparator struct{}

func (cmp *boolComparator) Equal(v1, v2 *driver.GenericColumnValue) (bool, error) {
	return v1.Bool() == v2.Bool(), nil
}

type bytesComparator struct{}

func (cmp *bytesComparator) Equal(v1, v2 *driver.GenericColumnValue) (bool, error) {
	b1, ok1 := v1.Value.([]byte)
	b2, ok2 := v1.Value.([]byte)
	if !ok1 || !ok2 {
		return false, fmt.Errorf("values are not convertible as []byte; v1: %T, v2: %T", v1, v2)
	}
	return bytes.Equal(b1, b2), nil
}
