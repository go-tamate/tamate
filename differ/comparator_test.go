package differ

import (
	"github.com/Mitu217/tamate/datasource"
	"testing"
)

func newTmpColumn(type_ datasource.ColumnType) *datasource.Column {
	return &datasource.Column{Type: type_, Name: "tmp"}
}

func newTmpGenericColumnValue(type_ datasource.ColumnType, value interface{}) *datasource.GenericColumnValue {
	return &datasource.GenericColumnValue{
		Column: newTmpColumn(type_),
		Value:  value,
	}
}

func TestAsStringComparator(t *testing.T) {
	cmp := &asStringComparator{}

	// int
	{
		v1 := newTmpGenericColumnValue(datasource.ColumnTypeInt, 12345)
		v2 := newTmpGenericColumnValue(datasource.ColumnTypeString, "12345")
		col := newTmpColumn(datasource.ColumnTypeInt)
		if eq, err := cmp.Equal(col, v1, v2); err != nil || !eq {
			t.Fatalf("12345 (int) == '12345' (string) must be true, but not equals")
		}
	}

	// float
	{
		v1 := newTmpGenericColumnValue(datasource.ColumnTypeFloat, 123.45)
		v2 := newTmpGenericColumnValue(datasource.ColumnTypeString, "123.45")
		col := newTmpColumn(datasource.ColumnTypeInt)
		if eq, err := cmp.Equal(col, v1, v2); err != nil || !eq {
			t.Fatalf("123.45 (float) == '123.45' (string) must be true, but not equals")
		}
	}
}

func TestBoolComparator(t *testing.T) {
	cmp := &boolComparator{}

	// by boolean string
	{
		v1 := newTmpGenericColumnValue(datasource.ColumnTypeBool, true)
		v2 := newTmpGenericColumnValue(datasource.ColumnTypeString, "true")
		col := newTmpColumn(datasource.ColumnTypeBool)
		if eq, err := cmp.Equal(col, v1, v2); err != nil || !eq {
			t.Fatalf("true(bool) == 'true' (string) must be true, but not equals")
		}
	}

	// by numeric string
	{
		v1 := newTmpGenericColumnValue(datasource.ColumnTypeBool, true)
		v2 := newTmpGenericColumnValue(datasource.ColumnTypeString, "1")
		col := newTmpColumn(datasource.ColumnTypeBool)
		if eq, err := cmp.Equal(col, v1, v2); err != nil || !eq {
			t.Fatalf("true(bool) == '1' (string) must be true, but not equals")
		}
	}
}
