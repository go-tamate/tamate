package schema

import (
	"io"
	"encoding/json"
	"os"
)

func NewJsonSchema(r io.Reader) (*Schema, error) {
	var sc *Schema
	if err := json.NewDecoder(r).Decode(&sc); err != nil {
		return nil, err
	}
	return sc, nil
}

func NewJsonFileSchema(path string) (*Schema, error) {
	r, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return NewJsonSchema(r)
}