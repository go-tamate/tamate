package datasource

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/araddon/dateparse"
)

type Datasource interface {
	GetSchema(ctx context.Context, name string) (*Schema, error)
	SetSchema(ctx context.Context, sc *Schema) error
	GetRows(ctx context.Context, sc *Schema) ([]*Row, error)
	SetRows(ctx context.Context, sc *Schema, rows []*Row) error
}

/*
 * Key
 */

const (
	KeyTypePrimary KeyType = iota
	KeyTypeUnique
	KeyTypeIndex
)

type KeyType int

type Key struct {
	KeyType     KeyType
	ColumnNames []string
}

func (k *Key) String() string {
	return fmt.Sprintf("%d:%v", k.KeyType, strings.Join(k.ColumnNames, ","))
}

/*
 * Column
 */

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

func (ct ColumnType) IsArray() bool {
	switch ct {
	case ColumnTypeBoolArray, ColumnTypeBytesArray, ColumnTypeDateArray, ColumnTypeDatetimeArray,
		ColumnTypeFloatArray, ColumnTypeIntArray, ColumnTypeStringArray:
		return true
	}
	return false
}

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

type Column struct {
	Name            string
	OrdinalPosition int
	Type            ColumnType
	NotNull         bool
	AutoIncrement   bool
}

func (c *Column) String() string {
	return fmt.Sprintf("%s %s", c.Name, c.Type)
}

/*
 * Schema
 */

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

// GetColumnNames is return name list of columns
func (sc *Schema) GetColumnNames() []string {
	var colNames []string
	for _, col := range sc.Columns {
		colNames = append(colNames, col.Name)
	}
	return colNames
}

/*
 * Row
 */

type RowValues map[string]*GenericColumnValue

type GroupByKey map[string][]*GenericColumnValue

type Row struct {
	GroupByKey GroupByKey
	Values     RowValues
}

func (r *Row) String() string {
	var sentences []string
	for key := range r.Values {
		sentences = append(sentences, fmt.Sprintf("%s: %+v", key, r.Values[key].StringValue()))
	}
	return "{" + strings.Join(sentences, ", ") + "}"
}

/*
 * Column Value
 */

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
	if cv.Column.Type.IsArray() {
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
