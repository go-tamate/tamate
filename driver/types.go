package driver

import "fmt"

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
		return fmt.Sprintf("<unknown column type: %d>", ct)
	}
}

func (ct ColumnType) IsArray() bool {
	switch ct {
	case ColumnTypeStringArray, ColumnTypeIntArray, ColumnTypeFloatArray, ColumnTypeDatetimeArray,
		ColumnTypeDateArray, ColumnTypeBytesArray, ColumnTypeBoolArray:
		return true
	default:
		return false
	}
}

type Column struct {
	Name          string
	Ordinal       int
	Type          ColumnType
	NotNull       bool
	AutoIncrement bool
}

func NewColumn(name string, ordinal int, ct ColumnType, notNull bool, authIncrement bool) *Column {
	return &Column{
		Name:          name,
		Ordinal:       ordinal,
		Type:          ct,
		NotNull:       notNull,
		AutoIncrement: authIncrement,
	}
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
		return fmt.Sprintf("<unknown key type: %d>", kt)
	}
}

type Key struct {
	KeyType     KeyType
	ColumnNames []string
}

func NewKey(kt KeyType, ColumnNames []string) *Key {
	return &Key{
		KeyType:     kt,
		ColumnNames: ColumnNames,
	}
}

type Schema struct {
	Name       string
	PrimaryKey *Key
	Columns    []*Column
}
