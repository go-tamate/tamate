package tamate

import (
	"testing"
)

func newTmpColumn(type_ ColumnType) *Column {
	return &Column{Type: type_, Name: "tmp"}
}

func newTmpGenericColumnValue(type_ ColumnType, value interface{}) *GenericColumnValue {
	return &GenericColumnValue{
		Column: newTmpColumn(type_),
		Value:  value,
	}
}

func TestAsStringComparator(t *testing.T) {
	cmp := &asStringComparator{}

	// int
	{
		v1 := newTmpGenericColumnValue(ColumnTypeInt, 12345)
		v2 := newTmpGenericColumnValue(ColumnTypeString, "12345")
		if eq, err := cmp.Equal(v1, v2); err != nil || !eq {
			t.Fatalf("12345 (int) == '12345' (string) must be true, but not equals")
		}
	}

	// float
	{
		v1 := newTmpGenericColumnValue(ColumnTypeFloat, 123.45)
		v2 := newTmpGenericColumnValue(ColumnTypeString, "123.45")
		if eq, err := cmp.Equal(v1, v2); err != nil || !eq {
			t.Fatalf("123.45 (float) == '123.45' (string) must be true, but not equals")
		}
	}

	// []string
	{
		v1 := newTmpGenericColumnValue(ColumnTypeStringArray, []string{"123", "456"})
		v2 := newTmpGenericColumnValue(ColumnTypeIntArray, []int64{123, 456})
		t.Logf("%+v", v1.StringValue())
		t.Logf("%+v", v2.StringValue())
		if eq, err := cmp.Equal(v1, v2); err != nil || !eq {
			t.Fatalf("['123', '456'] ([]string) == [123, 456] ([]int64) must be true, but not equals")
		}
	}

	// []string (comma-separated)
	{
		v1 := newTmpGenericColumnValue(ColumnTypeString, "123,456,-789")
		v2 := newTmpGenericColumnValue(ColumnTypeIntArray, []int64{123, 456, -789})
		if eq, err := cmp.Equal(v1, v2); err != nil || !eq {
			t.Fatalf("'123,456,-789' (string) == [123, 456, -789] ([]int64) must be true, but not equals")
		}
	}
}

func TestBoolComparator(t *testing.T) {
	cmp := &boolComparator{}

	// by boolean string
	{
		v1 := newTmpGenericColumnValue(ColumnTypeBool, true)
		v2 := newTmpGenericColumnValue(ColumnTypeString, "true")
		if eq, err := cmp.Equal(v1, v2); err != nil || !eq {
			t.Fatalf("true(bool) == 'true' (string) must be true, but not equals")
		}
	}

	// by numeric string
	{
		v1 := newTmpGenericColumnValue(ColumnTypeBool, true)
		v2 := newTmpGenericColumnValue(ColumnTypeString, "1")
		if eq, err := cmp.Equal(v1, v2); err != nil || !eq {
			t.Fatalf("true(bool) == '1' (string) must be true, but not equals")
		}
	}
}
