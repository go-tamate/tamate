package datasource

import (
	"fmt"
	"time"

	"github.com/araddon/dateparse"
	"reflect"
	"strconv"
	"strings"
)

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
	val := reflect.ValueOf(cv.Value)
	if cv.Column.IsArrayType() {
		kind := val.Kind()
		if kind == reflect.Array || kind == reflect.Slice {
			vlen := val.Len()
			ss := make([]string, vlen)
			for i := 0; i < vlen; i++ {
				ss[i] = fmt.Sprintf("%v", val.Index(i).Interface())
			}
			return strings.Join(ss, ",")
		}
	}
	return fmt.Sprintf("%v", cv.Value)
}

func (cv *GenericColumnValue) TimeValue() (time.Time, error) {
	switch cv.Value.(type) {
	case time.Time:
		return cv.Value.(time.Time), nil
	default:
		tv, err := dateparse.ParseAny(cv.StringValue())
		if err != nil {
			return time.Time{}, err
		}
		return tv, nil
	}
}

func (cv *GenericColumnValue) BoolValue() bool {
	switch cv.Value.(type) {
	case bool:
		return cv.Value.(bool)
	default:
		s := cv.StringValue()
		if strings.ToLower(s) == "true" {
			return true
		}
		num, err := strconv.Atoi(s)
		if err == nil {
			return num != 0
		}
	}
	return false
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
