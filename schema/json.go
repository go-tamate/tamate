package schema

import (
	"encoding/json"
	"io"
	"os"
)

func NewJsonSchema(r io.Reader) (*JsonSchema, error) {
	var sc *JsonSchema
	if err := json.NewDecoder(r).Decode(&sc); err != nil {
		return nil, err
	}
	return sc, nil
}

func NewJsonFileSchema(path string) (*JsonSchema, error) {
	r, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return NewJsonSchema(r)
}

func (sc *JsonSchema) GetColumns() []Column {
	return sc.Columns
}

func (sc *JsonSchema) GetTableName() string {
	return sc.Table.Name
}

func (sc *JsonSchema) Output() error {
	return nil
}

type JsonSchema struct {
	Description string   `json:"description"`
	Table       Table    `json:"table"`
	Columns     []Column `json:"properties"`
}
