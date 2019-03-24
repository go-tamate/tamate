package driver

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func Test_ColumnTypeToString(t *testing.T) {
	const columnTypeUnknown ColumnType = -1
	assert.Equal(t, "<unknown column type: -1>", columnTypeUnknown.String())

	assert.Equal(t, "<null>", ColumnTypeNull.String())
	assert.Equal(t, "string", ColumnTypeString.String())
	assert.Equal(t, "int", ColumnTypeInt.String())
	assert.Equal(t, "float", ColumnTypeFloat.String())
	assert.Equal(t, "datetime", ColumnTypeDatetime.String())
	assert.Equal(t, "date", ColumnTypeDate.String())
	assert.Equal(t, "bytes", ColumnTypeBytes.String())
	assert.Equal(t, "bool", ColumnTypeBool.String())

	assert.Equal(t, "array<string>", ColumnTypeStringArray.String())
	assert.Equal(t, "array<int>", ColumnTypeIntArray.String())
	assert.Equal(t, "array<float>", ColumnTypeFloatArray.String())
	assert.Equal(t, "array<datetime>", ColumnTypeDatetimeArray.String())
	assert.Equal(t, "array<date>", ColumnTypeDateArray.String())
	assert.Equal(t, "array<bytes>", ColumnTypeBytesArray.String())
	assert.Equal(t, "array<bool>", ColumnTypeBoolArray.String())
}

func Test_IsArrayColumnType(t *testing.T) {
	const ColumnTypeUnknown ColumnType = -1
	assert.Equal(t, false, ColumnTypeUnknown.IsArray())

	assert.Equal(t, false, ColumnTypeNull.IsArray())
	assert.Equal(t, false, ColumnTypeString.IsArray())
	assert.Equal(t, false, ColumnTypeInt.IsArray())
	assert.Equal(t, false, ColumnTypeFloat.IsArray())
	assert.Equal(t, false, ColumnTypeDatetime.IsArray())
	assert.Equal(t, false, ColumnTypeDate.IsArray())
	assert.Equal(t, false, ColumnTypeBytes.IsArray())
	assert.Equal(t, false, ColumnTypeBool.IsArray())

	assert.Equal(t, true, ColumnTypeStringArray.IsArray())
	assert.Equal(t, true, ColumnTypeIntArray.IsArray())
	assert.Equal(t, true, ColumnTypeFloatArray.IsArray())
	assert.Equal(t, true, ColumnTypeDatetimeArray.IsArray())
	assert.Equal(t, true, ColumnTypeDateArray.IsArray())
	assert.Equal(t, true, ColumnTypeBytesArray.IsArray())
	assert.Equal(t, true, ColumnTypeBoolArray.IsArray())
}

func Test_KeyToString(t *testing.T) {
	const KeyTypeUnknown KeyType = -1
	assert.Equal(t, "<unknown key type: -1>", KeyTypeUnknown.String())

	assert.Equal(t, "PrimaryKey", KeyTypePrimary.String())
	assert.Equal(t, "UniqueKey", KeyTypeUnique.String())
	assert.Equal(t, "Index", KeyTypeIndex.String())
}
