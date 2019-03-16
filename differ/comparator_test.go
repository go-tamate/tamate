package differ

import (
	"testing"

	"github.com/go-tamate/tamate/driver"
	"github.com/stretchr/testify/assert"
)

func newTmpColumn(columnType driver.ColumnType) *driver.Column {
	return &driver.Column{Type: columnType, Name: "tmp"}
}

func newTmpGenericColumnValue(columnType driver.ColumnType, value interface{}) *driver.GenericColumnValue {
	return &driver.GenericColumnValue{
		Column: newTmpColumn(columnType),
		Value:  value,
	}
}

func Test_AsStringComparator(t *testing.T) {
	cmp := &asStringComparator{}

	// int
	{
		v1 := newTmpGenericColumnValue(driver.ColumnTypeInt, 12345)
		v2 := newTmpGenericColumnValue(driver.ColumnTypeString, "12345")
		eq, err := cmp.Equal(v1, v2)
		if assert.NoError(t, err) {
			assert.True(t, eq)
		}
	}

	// float
	{
		v1 := newTmpGenericColumnValue(driver.ColumnTypeFloat, 123.45)
		v2 := newTmpGenericColumnValue(driver.ColumnTypeString, "123.45")
		eq, err := cmp.Equal(v1, v2)
		if assert.NoError(t, err) {
			assert.True(t, eq)
		}
	}

	// []string
	{
		v1 := newTmpGenericColumnValue(driver.ColumnTypeStringArray, []string{"123", "456"})
		v2 := newTmpGenericColumnValue(driver.ColumnTypeIntArray, []int64{123, 456})
		eq, err := cmp.Equal(v1, v2)
		if assert.NoError(t, err) {
			assert.True(t, eq)
		}
	}

	// []string (comma-separated)
	{
		v1 := newTmpGenericColumnValue(driver.ColumnTypeString, "[123, 456, -789]")
		v2 := newTmpGenericColumnValue(driver.ColumnTypeIntArray, []int64{123, 456, -789})
		eq, err := cmp.Equal(v1, v2)
		if assert.NoError(t, err) {
			assert.True(t, eq)
		}
	}
}

func TestBoolComparator(t *testing.T) {
	cmp := &boolComparator{}

	// by boolean string
	{
		v1 := newTmpGenericColumnValue(driver.ColumnTypeBool, true)
		v2 := newTmpGenericColumnValue(driver.ColumnTypeString, "true")
		eq, err := cmp.Equal(v1, v2)
		if assert.NoError(t, err) {
			assert.True(t, eq)
		}
	}

	// by numeric string
	{
		v1 := newTmpGenericColumnValue(driver.ColumnTypeBool, true)
		v2 := newTmpGenericColumnValue(driver.ColumnTypeString, "1")
		eq, err := cmp.Equal(v1, v2)
		if assert.NoError(t, err) {
			assert.True(t, eq)
		}
	}
}
