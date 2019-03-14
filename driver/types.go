package driver

import (
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/araddon/dateparse"
)

const (
	ColumnTypeNull ColumnType = iota
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

type ColumnType int

func (ct ColumnType) String() string {
	switch ct {
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
		return fmt.Sprintf("<unknown type: %d>", ct)
	}
}

func (ct ColumnType) IsArray() bool {
	return strings.Index(ct.String(), "array") > -1
}

type Column struct {
	Name            string
	OrdinalPosition int
	Type            ColumnType
	NotNull         bool
	AutoIncrement   bool
}

func NewColumn(name string, ordinalPosition int, columnType ColumnType, notNull bool, authIncrement bool) *Column {
	return &Column{
		Name:            name,
		OrdinalPosition: ordinalPosition,
		Type:            columnType,
		NotNull:         notNull,
		AutoIncrement:   authIncrement,
	}
}

func (c *Column) String() string {
	return fmt.Sprintf("%s %s", c.Name, c.Type)
}

type GenericColumnValue struct {
	Column *Column
	Value  interface{}
}

func NewGenericColumnValue(column *Column, value interface{}) *GenericColumnValue {
	return &GenericColumnValue{
		Column: column,
		Value:  value,
	}
}

// Deprecated:
func (cv *GenericColumnValue) StringValue() string {
	return cv.String()
}

func (cv *GenericColumnValue) String() string {
	val := reflect.ValueOf(cv.Value)
	if cv.Column.Type.IsArray() {
		kind := val.Kind()
		if kind == reflect.Array || kind == reflect.Slice {
			vlen := val.Len()
			ss := make([]string, vlen)
			for i := 0; i < vlen; i++ {
				ss[i] = fmt.Sprintf("%v", val.Index(i).Interface())
			}
			return "[" + strings.Join(ss, ", ") + "]"
		}
	}
	return fmt.Sprintf("%v", cv.Value)
}

// Deprecated:
func (cv *GenericColumnValue) TimeValue() (time.Time, error) {
	return cv.Time(), nil
}

func (cv *GenericColumnValue) Time() time.Time {
	switch cv.Value.(type) {
	case time.Time:
		return cv.Value.(time.Time)
	default:
		tv, err := dateparse.ParseAny(cv.String())
		if err != nil {
			return time.Time{}
		}
		return tv
	}
}

// Deprecated:
func (cv *GenericColumnValue) BoolValue() bool {
	return cv.Bool()
}

func (cv *GenericColumnValue) Bool() bool {
	switch cv.Value.(type) {
	case bool:
		return cv.Value.(bool)
	default:
		s := cv.String()
		if strings.ToLower(s) == "true" {
			return true
		}
		if strings.ToLower(s) == "false" {
			return false
		}
		num, err := strconv.Atoi(s)
		if err == nil {
			return num != 0
		}
	}
	return false
}

type RowValues map[string]*GenericColumnValue

type GroupByKey map[string][]*GenericColumnValue

type Row struct {
	GroupByKey GroupByKey
	Values     RowValues
}

func (r *Row) String() string {
	// Sorting by OrdinalPosition
	keys := make([]string, 0, len(r.Values))
	for k, val := range r.Values {
		pos := val.Column.OrdinalPosition
		keys[pos] = k
	}

	var sentences []string
	for _, key := range keys {
		sentences = append(sentences, fmt.Sprintf("%s: %+v", key, r.Values[key].String()))
	}
	return "{" + strings.Join(sentences, ", ") + "}"
}

const (
	KeyTypePrimary KeyType = iota
	KeyTypeUnique
	KeyTypeIndex
)

type KeyType int

func (kt KeyType) String() string {
	switch kt {
	case KeyTypePrimary:
		return "PrimaryKey"
	case KeyTypeUnique:
		return "UniqueKey"
	case KeyTypeIndex:
		return "Index"
	default:
		return fmt.Sprintf("<unknown type: %d>", kt)
	}
}

type Key struct {
	KeyType     KeyType
	ColumnNames []string
}

func (k *Key) String() string {
	return fmt.Sprintf("%s:%v", k.KeyType, strings.Join(k.ColumnNames, ","))
}

type Schema struct {
	Name       string
	PrimaryKey *Key
	Columns    []*Column
}

func (sc *Schema) String() string {
	var sts []string
	for _, c := range sc.Columns {
		sts = append(sts, c.String())
	}
	return fmt.Sprintf("%s(%s) PK=(%s)", sc.Name, strings.Join(sts, ", "), sc.PrimaryKey)
}
