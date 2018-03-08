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

type JsonSchema struct {
	Description string   `json:"description"`
	Table       Table    `json:"table"`
	Properties  []Column `json:"properties"`
}
