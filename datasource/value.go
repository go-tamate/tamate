package datasource

import "fmt"

type GenericColumnValue struct {
	ColumnType ColumnType
	Value      interface{}
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
	ColumnType_Null = ColumnType(iota)
	ColumnType_String
	ColumnType_Int
	ColumnType_Float
	ColumnType_Datetime
	ColumnType_Date
	ColumnType_Bytes
	ColumnType_Bool
)

func (vt ColumnType) String() string {
	switch vt {
	case ColumnType_Null:
		return "<null>"
	case ColumnType_String:
		return "string"
	case ColumnType_Int:
		return "int"
	case ColumnType_Float:
		return "float"
	case ColumnType_Datetime:
		return "datetime"
	case ColumnType_Date:
		return "date"
	case ColumnType_Bytes:
		return "bytes"
	case ColumnType_Bool:
		return "bool"
	default:
		return fmt.Sprintf("<unknown type: %d>", vt)
	}
}

func newStringValue(value string) *GenericColumnValue {
	return &GenericColumnValue{
		ColumnType: ColumnType_String,
		Value:      value,
	}
}

func newFloatValue(value float64) *GenericColumnValue {
	return &GenericColumnValue{
		ColumnType: ColumnType_Float,
		Value:      value,
	}
}
