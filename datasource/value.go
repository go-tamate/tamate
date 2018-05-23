package datasource

import "fmt"

type GenericColumnValue struct {
	ColumnType ColumnType
	Value      interface{}
	Nullable   bool
}

func (cv *GenericColumnValue) StringValue() string {
	switch cv.ColumnType {
	// TODO: additional string reprensentation for specific value type
	default:
		return fmt.Sprintf("%s", cv.Value)
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
	default:
		return fmt.Sprintf("<unknown type: %d>", vt)
	}
}

func newStringValue(value string) *GenericColumnValue {
	return &GenericColumnValue{
		ColumnType: ColumnTypeString,
		Value:      value,
	}
}
