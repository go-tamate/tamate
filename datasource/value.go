package datasource

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/araddon/dateparse"
)

// TODO: rename GenericRowValue?
type GenericColumnValue struct {
	Column *Column
	Value  interface{}
}

func NewGenericColumnValue(col *Column) *GenericColumnValue {
	switch col.Type {
	case ColumnTypeString:
		return NewStringGenericColumnValue(col, "")
	}
	return nil
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
