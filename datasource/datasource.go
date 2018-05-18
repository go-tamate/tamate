package datasource

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

type PrimaryKey struct {
	ColumnNames []string `json:"column_names"`
}

// Schema is column definitions at table
type Schema struct {
	Name       string      `json:"name"`
	PrimaryKey *PrimaryKey `json:"primary_key"`
	Columns    []*Column   `json:"columns"`
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
	Open() error
	Close() error
	GetSchemas() ([]*Schema, error)
	GetSchema(string) (*Schema, error)
	SetSchema(*Schema) error
	GetRows(*Schema) (*Rows, error)
	SetRows(*Schema, *Rows) error
}
