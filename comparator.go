package tamate

import (
	"bytes"
	"fmt"
)

type ComparatorMap map[ColumnType]ValueComparator

func NewComparatorMap() ComparatorMap {
	cm := make(ComparatorMap)

	cm[ColumnTypeDatetime] = &datetimeComparator{}
	cm[ColumnTypeBool] = &boolComparator{}
	cm[ColumnTypeBytes] = &bytesComparator{}

	cm[ColumnTypeString] = &asStringComparator{}
	cm[ColumnTypeInt] = &asStringComparator{}
	cm[ColumnTypeFloat] = &asStringComparator{}
	cm[ColumnTypeDate] = &asStringComparator{}

	// TODO: Implement type optimized comparator
	cm[ColumnTypeStringArray] = &asStringComparator{}
	cm[ColumnTypeBytesArray] = &asStringComparator{}
	cm[ColumnTypeFloatArray] = &asStringComparator{}
	cm[ColumnTypeIntArray] = &asStringComparator{}
	cm[ColumnTypeDateArray] = &asStringComparator{}
	cm[ColumnTypeDatetimeArray] = &asStringComparator{}
	cm[ColumnTypeBoolArray] = &asStringComparator{}

	return cm
}

func (cmap ComparatorMap) Equal(v1, v2 *GenericColumnValue) (bool, error) {
	if cmp, has := cmap[v1.Column.Type]; has {
		return cmp.Equal(v1, v2)
	}
	return v1.Value == v2.Value, nil
}

type ValueComparator interface {
	Equal(v1, v2 *GenericColumnValue) (bool, error)
}

type datetimeComparator struct{}

func (cmp *datetimeComparator) Equal(v1, v2 *GenericColumnValue) (bool, error) {
	// If error occurred, empty time struct is returned.
	// So we should just compare time.Time simply.
	ltv, _ := v1.TimeValue()
	rtv, _ := v2.TimeValue()
	return ltv == rtv, nil
}

type asStringComparator struct{}

func (cmp *asStringComparator) Equal(v1, v2 *GenericColumnValue) (bool, error) {
	return v1.StringValue() == v2.StringValue(), nil
}

type boolComparator struct{}

func (cmp *boolComparator) Equal(v1, v2 *GenericColumnValue) (bool, error) {
	return v1.BoolValue() == v2.BoolValue(), nil
}

type bytesComparator struct{}

func (cmp *bytesComparator) Equal(v1, v2 *GenericColumnValue) (bool, error) {
	b1, ok1 := v1.Value.([]byte)
	b2, ok2 := v1.Value.([]byte)
	if !ok1 || !ok2 {
		return false, fmt.Errorf("values are not convertible as []byte; v1: %T, v2: %T", v1, v2)
	}
	return bytes.Equal(b1, b2), nil
}
