package datasource

import "fmt"

type GenericColumnValue struct {
	Column *Column
	Value  interface{}
}

func NewStringGenericColumnValue(col *Column, s string) *GenericColumnValue {
	return &GenericColumnValue{
		Column: col,
		Value:  s,
	}
}

func (cv *GenericColumnValue) StringValue() string {
	switch cv.Column.Type {
	// TODO: additional string reprensentation for specific value type
	default:
		return fmt.Sprintf("%v", cv.Value)
	}
}

type ColumnType int

const (
	ColumnTypeNull = ColumnType(iota)
	ColumnTypeString
	ColumnTypeInt
	ColumnTypeFloat
	ColumnTypeDatetime
	ColumnTypeDate
	ColumnTypeBytes
	ColumnTypeBool
	ColumnTypeStringArray
	ColumnTypeIntArray
	ColumnTypeFloatArray
	ColumnTypeDatetimeArray
	ColumnTypeDateArray
	ColumnTypeBytesArray
	ColumnTypeBoolArray
)

func (vt ColumnType) String() string {
	switch vt {
	case ColumnTypeNull:
		return "<null>"
	case ColumnTypeString:
		return "string"
	case ColumnTypeInt:
		return "int"
	case ColumnTypeFloat:
		return "float"
	case ColumnTypeDatetime:
		return "datetime"
	case ColumnTypeDate:
		return "date"
	case ColumnTypeBytes:
		return "bytes"
	case ColumnTypeBool:
		return "bool"
	case ColumnTypeStringArray:
		return "array<string>"
	case ColumnTypeIntArray:
		return "array<int>"
	case ColumnTypeFloatArray:
		return "array<float>"
	case ColumnTypeDatetimeArray:
		return "array<datetime>"
	case ColumnTypeDateArray:
		return "array<date>"
	case ColumnTypeBytesArray:
		return "array<bytes>"
	case ColumnTypeBoolArray:
		return "array<bool>"
	default:
		return fmt.Sprintf("<unknown type: %d>", vt)
	}
}
