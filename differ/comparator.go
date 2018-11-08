package differ

import (
	"bytes"
	"fmt"

	"github.com/Mitu217/tamate/datasource"
)

type ComparatorMap map[datasource.ColumnType]ValueComparator

func NewComparatorMap() ComparatorMap {
	cm := make(ComparatorMap)

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

func (cmap ComparatorMap) Equal(v1, v2 *datasource.GenericColumnValue) (bool, error) {
	if cmp, has := cmap[v1.Column.Type]; has {
		return cmp.Equal(v1, v2)
	}
	return v1.Value == v2.Value, nil
}

type ValueComparator interface {
	Equal(v1, v2 *datasource.GenericColumnValue) (bool, error)
}

type datetimeComparator struct{}

func (cmp *datetimeComparator) Equal(v1, v2 *datasource.GenericColumnValue) (bool, error) {
	// If error occurred, empty time struct is returned.
	// So we should just compare time.Time simply.
	ltv, _ := v1.TimeValue()
	rtv, _ := v2.TimeValue()
	return ltv == rtv, nil
}

type asStringComparator struct{}

func (cmp *asStringComparator) Equal(v1, v2 *datasource.GenericColumnValue) (bool, error) {
	return v1.StringValue() == v2.StringValue(), nil
}

type boolComparator struct{}

func (cmp *boolComparator) Equal(v1, v2 *datasource.GenericColumnValue) (bool, error) {
	return v1.BoolValue() == v2.BoolValue(), nil
}

type bytesComparator struct{}

func (cmp *bytesComparator) Equal(v1, v2 *datasource.GenericColumnValue) (bool, error) {
	b1, ok1 := v1.Value.([]byte)
	b2, ok2 := v1.Value.([]byte)
	if !ok1 || !ok2 {
		return false, fmt.Errorf("values are not convertible as []byte; v1: %T, v2: %T", v1, v2)
	}
	return bytes.Equal(b1, b2), nil
}
