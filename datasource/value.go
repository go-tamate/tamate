package datasource

import (
	"fmt"
	"strconv"
)

type GenericColumnValue struct {
	ColumnType ColumnType
	Value      interface{}
}

func (cv *GenericColumnValue) StringValue() string {

	// @todo Find more better way to convert...
	switch cv.ColumnType {
	case ColumnTypeInt:
		switch cv.Value.(type) {
		case int:
			if i, ok := cv.Value.(int); ok {
				return strconv.Itoa(i)
			} else {
				return fmt.Sprintf("%d", cv.Value)
			}
		case int64:
			if i, ok := cv.Value.(int64); ok {
				return strconv.FormatInt(i, 10)
			} else {
				return fmt.Sprintf("%d", cv.Value)
			}
		case float64:
			if f, ok := cv.Value.(float64); ok {
				return strconv.FormatFloat(f, 'f', -1, 64)
			} else {
				return fmt.Sprintf("%f", cv.Value)
			}
		default:
			return fmt.Sprintf("%d", cv.Value)
		}
	case ColumnTypeFloat:
		if f, ok := cv.Value.(float64); ok {
			return strconv.FormatFloat(f, 'f', -1, 64)
		} else {
			return fmt.Sprintf("%f", cv.Value)
		}
	case ColumnTypeBool:
		return fmt.Sprintf("%t", cv.Value)
	case ColumnTypeDatetime:
		fallthrough
	case ColumnTypeDate:
		fallthrough
	case ColumnTypeBytes:
		fallthrough
	case ColumnTypeNull:
		fallthrough
	case ColumnTypeString:
		fallthrough
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
