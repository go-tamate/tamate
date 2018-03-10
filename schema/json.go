package schema

import (
	"encoding/json"
	"io"
	"os"
)

// JSONSchema :
type JSONSchema struct {
	Description string   `json:"description"`
	Table       Table    `json:"table"`
	Columns     []Column `json:"properties"`
}

// NewJSONSchema :
func NewJSONSchema(r io.Reader) (*JSONSchema, error) {
	var sc *JSONSchema
	if err := json.NewDecoder(r).Decode(&sc); err != nil {
		return nil, err
	}
	return sc, nil
}

// NewJSONFileSchema :
func NewJSONFileSchema(path string) (*JSONSchema, error) {
	r, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return NewJSONSchema(r)
}

// GetPrimaryKey :
func (sc *JSONSchema) GetPrimaryKey() string {
	return sc.Table.PrimaryKey
}

// GetColumns :
func (sc *JSONSchema) GetColumns() []Column {
	return sc.Columns
}

// GetColumnNames :
func (sc *JSONSchema) GetColumnNames() []string {
	columnNames := make([]string, len(sc.Columns))
	for i := range sc.Columns {
		columnNames[i] = sc.Columns[i].Name
	}
	return columnNames
}

// GetTableName :
func (sc *JSONSchema) GetTableName() string {
	return sc.Table.Name
}

// Output :
func (sc *JSONSchema) Output() error {
	return nil
}
