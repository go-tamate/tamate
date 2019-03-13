package driver

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func Test_ColumnTypeToString(t *testing.T) {
	// unknown
	const columnTypeUnknown ColumnType = -1
	assert.Equal(t, "<unknown type: -1>", columnTypeUnknown.String())

	// primary
	assert.Equal(t, "<null>", ColumnTypeNull.String())
	assert.Equal(t, "string", ColumnTypeString.String())
	assert.Equal(t, "int", ColumnTypeInt.String())
	assert.Equal(t, "float", ColumnTypeFloat.String())
	assert.Equal(t, "datetime", ColumnTypeDatetime.String())
	assert.Equal(t, "date", ColumnTypeDate.String())
	assert.Equal(t, "bytes", ColumnTypeBytes.String())
	assert.Equal(t, "bool", ColumnTypeBool.String())

	// array
	assert.Equal(t, "array<string>", ColumnTypeStringArray.String())
	assert.Equal(t, "array<int>", ColumnTypeIntArray.String())
	assert.Equal(t, "array<float>", ColumnTypeFloatArray.String())
	assert.Equal(t, "array<datetime>", ColumnTypeDatetimeArray.String())
	assert.Equal(t, "array<date>", ColumnTypeDateArray.String())
	assert.Equal(t, "array<bytes>", ColumnTypeBytesArray.String())
	assert.Equal(t, "array<bool>", ColumnTypeBoolArray.String())
}

func Test_IsArrayColumnType(t *testing.T) {
	// unknown
	const columnTypeUnknown ColumnType = -1
	assert.Equal(t, false, columnTypeUnknown.IsArray())

	// primary
	assert.Equal(t, false, ColumnTypeNull.IsArray())
	assert.Equal(t, false, ColumnTypeString.IsArray())
	assert.Equal(t, false, ColumnTypeInt.IsArray())
	assert.Equal(t, false, ColumnTypeFloat.IsArray())
	assert.Equal(t, false, ColumnTypeDatetime.IsArray())
	assert.Equal(t, false, ColumnTypeDate.IsArray())
	assert.Equal(t, false, ColumnTypeBytes.IsArray())
	assert.Equal(t, false, ColumnTypeBool.IsArray())

	// array
	assert.Equal(t, true, ColumnTypeStringArray.IsArray())
	assert.Equal(t, true, ColumnTypeIntArray.IsArray())
	assert.Equal(t, true, ColumnTypeFloatArray.IsArray())
	assert.Equal(t, true, ColumnTypeDatetimeArray.IsArray())
	assert.Equal(t, true, ColumnTypeDateArray.IsArray())
	assert.Equal(t, true, ColumnTypeBytesArray.IsArray())
	assert.Equal(t, true, ColumnTypeBoolArray.IsArray())
}

func Test_ColumnToString(t *testing.T) {
	col := NewColumn("test", 0, ColumnTypeString, false, false)
	assert.Equal(t, fmt.Sprintf("%s %s", col.Name, col.Type), col.String())
}

func Test_GenericColumnValueToString(t *testing.T) {
	var (
		col *Column
		cv  *GenericColumnValue
	)

	// string
	col = NewColumn("string", 0, ColumnTypeString, false, false)
	cv = NewGenericColumnValue(col, "this is a pen")
	assert.Equal(t, "this is a pen", cv.String())

	// array<string>
	col = NewColumn("array<string>", 0, ColumnTypeStringArray, false, false)
	cv = NewGenericColumnValue(col, []string{"one", "two", "three"})
	assert.Equal(t, "[one, two, three]", cv.String())

	// int
	col = NewColumn("int", 0, ColumnTypeInt, false, false)
	cv = NewGenericColumnValue(col, 0)
	assert.Equal(t, "0", cv.String())
}

func Test_GenericColumnValueToTime(t *testing.T) {
	var (
		col  *Column
		cv   *GenericColumnValue
		time = time.Date(2018, time.April, 1, 12, 13, 24, 0, time.UTC)
	)

	// time.Time
	col = NewColumn("datetime", 0, ColumnTypeDatetime, false, false)
	cv = NewGenericColumnValue(col, time)
	assert.Equal(t, time.String(), cv.Time().String())

	// time format string "YY-mm-dd HH:ii:ss"
	col = NewColumn("string format", 0, ColumnTypeDatetime, false, false)
	cv = NewGenericColumnValue(col, "2018-04-01 12:13:24")
	assert.Equal(t, time.String(), cv.Time().String())
}

func Test_GenericColumnValueToBool(t *testing.T) {
	var (
		col *Column
		cv  *GenericColumnValue
	)

	// bool
	col = NewColumn("bool", 0, ColumnTypeBool, false, false)
	cv = NewGenericColumnValue(col, true)
	assert.Equal(t, true, cv.Bool())
	cv = NewGenericColumnValue(col, false)
	assert.Equal(t, false, cv.Bool())

	// bool format string "true" or "false"
	col = NewColumn("string format", 0, ColumnTypeBool, false, false)
	cv = NewGenericColumnValue(col, "true")
	assert.Equal(t, true, cv.Bool())
	cv = NewGenericColumnValue(col, "TRUE")
	assert.Equal(t, true, cv.Bool())
	cv = NewGenericColumnValue(col, "false")
	assert.Equal(t, false, cv.Bool())
	cv = NewGenericColumnValue(col, "FALSE")
	assert.Equal(t, false, cv.Bool())
}

func Test_RowToString(t *testing.T) {
	var (
		fakeID = &GenericColumnValue{
			Column: NewColumn("id", 0, ColumnTypeInt, true, true),
			Value:  1,
		}
		fakeName = &GenericColumnValue{
			Column: NewColumn("name", 1, ColumnTypeString, true, false),
			Value:  "Hana",
		}
		fakeRow = &Row{
			Values: RowValues{
				"id":   fakeID,
				"name": fakeName,
			},
		}
	)

	assert.Equal(t, "{id: 1, name: Hana}", fakeRow.String())
}

func Test_KeyToString(t *testing.T) {
	// unknown
	const keyTypeUnknown KeyType = -1
	assert.Equal(t, "<unknown type: -1>", keyTypeUnknown.String())

	assert.Equal(t, "PrimaryKey", KeyTypePrimary.String())
	assert.Equal(t, "UniqueKey", KeyTypeUnique.String())
	assert.Equal(t, "Index", KeyTypeIndex.String())
}

func Test_SchemaToString(t *testing.T) {
	var (
		fakePK = &Key{
			KeyType:     KeyTypePrimary,
			ColumnNames: []string{"id"},
		}
		fakeIDColumn   = NewColumn("id", 0, ColumnTypeInt, true, true)
		fakeNameColumn = NewColumn("name", 1, ColumnTypeString, true, false)
		fakeSchema     = &Schema{
			Name:       "Class",
			PrimaryKey: fakePK,
			Columns:    []*Column{fakeIDColumn, fakeNameColumn},
		}
	)

	assert.Equal(t, "Class(id int, name string) PK=(PrimaryKey:id)", fakeSchema.String())
}
