package handler

// Column is table column
type Column struct {
	Name            string `json:"name"`
	OrdinalPosition int    `json:"ordinal_position"`
	Type            string `json:"type"`
	NotNull         bool   `json:"not_null"`
	AutoIncrement   bool   `json:"auto_increment"`
}

// Rows is table records
type Rows struct {
	Values [][]string `json:"values"`
}

// Schema is column definitions at table
type Schema struct {
	Name            string   `json:"name"`
	PrimaryKey      string   `json:"primary_key"`
	Columns         []Column `json:"columns"`
	primaryKeyIndex int
}

// NewSchema is create schema instance
func NewSchema(name string) (*Schema, error) {
	return &Schema{
		Name:            name,
		primaryKeyIndex: -1,
	}, nil
}

// GetPrimaryKeyIndex is return index of primary key
func (sc *Schema) GetPrimaryKeyIndex() int {
	return sc.primaryKeyIndex
}

// GetColumnNames is return name list of columns
func (sc *Schema) GetColumnNames() []string {
	var colNames []string
	for _, col := range sc.Columns {
		colNames = append(colNames, col.Name)
	}
	return colNames
}

// Handler is read and write datasource interface
type Handler interface {
	Open() error
	Close() error
	GetSchemas() ([]*Schema, error)
	GetSchema(*Schema) error
	SetSchema(*Schema) error
	GetRows(*Schema) (*Rows, error)
	SetRows(*Schema, *Rows) error
}
