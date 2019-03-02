package driver

import (
	"fmt"
	"reflect"
	"sort"
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
	default:
		return fmt.Sprintf("<unknown type: %d>", ct)
	}
}

type Column struct {
	Name            string
	OrdinalPosition int
	Type            ColumnType
	Array           bool
	NotNull         bool
	AutoIncrement   bool
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

func (cv *GenericColumnValue) StringValue() string {
	val := reflect.ValueOf(cv.Value)
	if cv.Column.Array {
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

func RowsToPrimaryKeyMap(schema *Schema, rows []*Row) (map[string]*Row, error) {
	primaryKeyString := schema.PrimaryKey.String()
	primaryKeyMap := make(map[string]*Row, len(rows))
	for _, row := range rows {
		columnValues, ok := row.GroupByKey[primaryKeyString]
		if !ok {
			return nil, fmt.Errorf("rows has no PK(%s) value", primaryKeyString)
		}
		var primaryValues []string
		for _, columnValue := range columnValues {
			primaryValues = append(primaryValues, columnValue.StringValue())
		}
		sort.Strings(primaryValues)
		k := strings.Join(primaryValues, "_")

		// completion column
		resRow := &Row{
			GroupByKey: row.GroupByKey,
			Values:     make(RowValues),
		}
		for _, column := range schema.Columns {
			for rowColumnName, columnValues := range row.Values {
				if rowColumnName == column.Name {
					resRow.Values[column.Name] = columnValues
					break
				}
			}
			if _, has := resRow.Values[column.Name]; !has {
				resRow.Values[column.Name] = NewGenericColumnValue(column, "")
			}
		}
		primaryKeyMap[k] = resRow
	}
	return primaryKeyMap, nil
}

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

func (sc *Schema) GetColumnNames() []string {
	var colNames []string
	for _, col := range sc.Columns {
		colNames = append(colNames, col.Name)
	}
	return colNames
}
