package datasource

import (
	"context"
	"fmt"
	"strings"
)

type Datasource interface {
	GetSchema(ctx context.Context, name string) (*Schema, error)
	SetSchema(ctx context.Context, sc *Schema) error
	GetRows(ctx context.Context, sc *Schema) ([]*Row, error)
	SetRows(ctx context.Context, sc *Schema, rows []*Row) error
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
	Name            string     `json:"name"`
	OrdinalPosition int        `json:"ordinal_position"`
	Type            ColumnType `json:"type"`
	NotNull         bool       `json:"not_null"`
	AutoIncrement   bool       `json:"auto_increment"`
}

func (c *Column) String() string {
	return fmt.Sprintf("%s %s", c.Name, c.Type)
}

func (c *Column) IsArrayType() bool {
	switch c.Type {
	case ColumnTypeBoolArray, ColumnTypeBytesArray, ColumnTypeDateArray, ColumnTypeDatetimeArray,
		ColumnTypeFloatArray, ColumnTypeIntArray, ColumnTypeStringArray:
		return true
	}
	return false
}

/*
 * Schema
 */
type Schema struct {
	Name       string    `json:"name"`
	PrimaryKey *Key      `json:"primary_key"`
	Columns    []*Column `json:"columns"`
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
	GroupByKey GroupByKey `json:"-"`
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
 * Key
 */

const (
	KeyTypePrimary KeyType = iota
	KeyTypeUnique
	KeyTypeIndex
)

type KeyType int

type Key struct {
	KeyType     KeyType  `json:"key_type"`
	ColumnNames []string `json:"column_names"`
}

func (k *Key) String() string {
	return fmt.Sprintf("%d:%v", k.KeyType, strings.Join(k.ColumnNames, ","))
}
