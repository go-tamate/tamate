package differ

import (
	"bytes"
	"fmt"

	"github.com/Mitu217/tamate/datasource"
)

type ValueComparator interface {
	Equal(col *datasource.Column, v1, v2 *datasource.GenericColumnValue) (bool, error)
}

type datetimeComparator struct{}

func (cmp *datetimeComparator) Equal(col *datasource.Column, v1, v2 *datasource.GenericColumnValue) (bool, error) {
	// If error occurred, empty time struct is returned.
	// So we should just compare time.Time simply.
	ltv, _ := v1.TimeValue()
	rtv, _ := v2.TimeValue()
	return ltv == rtv, nil
}

type asStringComparator struct{}

func (cmp *asStringComparator) Equal(col *datasource.Column, v1, v2 *datasource.GenericColumnValue) (bool, error) {
	return v1.StringValue() == v2.StringValue(), nil
}

type boolComparator struct{}

func (cmp *boolComparator) Equal(col *datasource.Column, v1, v2 *datasource.GenericColumnValue) (bool, error) {
	return v1.BoolValue() == v2.BoolValue(), nil
}

type bytesComparator struct{}

func (cmp *bytesComparator) Equal(col *datasource.Column, v1, v2 *datasource.GenericColumnValue) (bool, error) {
	b1, ok1 := v1.Value.([]byte)
	b2, ok2 := v1.Value.([]byte)
	if !ok1 || !ok2 {
		return false, fmt.Errorf("values are not convertible as []byte; v1: %T, v2: %T", v1, v2)
	}
	return bytes.Equal(b1, b2), nil
}
