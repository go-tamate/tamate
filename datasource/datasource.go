package datasource

import (
	"context"
	"fmt"
	"strings"
)

// Column is table column
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

type RowValues map[string]*GenericColumnValue

type Row struct {
	GroupByKey map[*Key][]*GenericColumnValue
	Values     RowValues
}

func (r *Row) String() string {
	var sts []string
	for cn, val := range r.Values {
		sts = append(sts, fmt.Sprintf("%s: %+v", cn, val.StringValue()))
	}
	return "{" + strings.Join(sts, ", ") + "}"
}

const (
	KeyTypePrimary = iota
	KeyTypeUnique
	KeyTypeIndex
)

type KeyType int

type Key struct {
	TableName   string   `json:"table_name"`
	KeyType     KeyType  `json:"key_type"`
	ColumnNames []string `json:"column_names"`
}

func (k *Key) String() string {
	return strings.Join(k.ColumnNames, ",")
}

// Schema is column definitions at table
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

// TODO: composite primary key support
func (sc *Schema) GetPrimaryKeyIndex() int {
	for i, col := range sc.Columns {
		if col.Name == sc.PrimaryKey.ColumnNames[0] {
			return i
		}
	}
	return -1
}

// GetColumnNames is return name list of columns
func (sc *Schema) GetColumnNames() []string {
	var colNames []string
	for _, col := range sc.Columns {
		colNames = append(colNames, col.Name)
	}
	return colNames
}

// Datasource is read and write datasource interface
type Datasource interface {
	GetAllSchema(ctx context.Context) ([]*Schema, error)
	GetSchema(ctx context.Context, name string) (*Schema, error)
	SetSchema(ctx context.Context, sc *Schema) error
	GetRows(ctx context.Context, sc *Schema) ([]*Row, error)
	SetRows(ctx context.Context, sc *Schema, rows []*Row) error
}
