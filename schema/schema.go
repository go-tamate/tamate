package schema

import (
	"encoding/json"
	"io"
	"os"
)

// Server :
type Server struct {
	DriverName string `json:"driver_name"`
	Host       string `json:"host"`
	Port       int    `json:"port"`
	User       string `json:"user"`
	Password   string `json:"password"`
}

// Table :
type Table struct {
	Name       string   `json:"name"`
	PrimaryKey string   `json:"primary_key"`
	UniqueKey  []string `json:"unique_key"`
}

// Column :
type Column struct {
	Name          string `json:"name"`
	Type          string `json:"type"`
	NotNull       bool   `json:"not_null"`
	AutoIncrement bool   `json:"auto_increment"`
}

// Schema :
type Schema struct {
	DatabaseName string   `json:"database"`
	Description  string   `json:"description"`
	Table        Table    `json:"table"`
	Columns      []Column `json:"properties"`
}

// NewJSONSchema :
func NewJSONSchema(r io.Reader) (*Schema, error) {
	var sc *Schema
	if err := json.NewDecoder(r).Decode(&sc); err != nil {
		return nil, err
	}
	return sc, nil
}

// NewJSONFileSchema :
func NewJSONFileSchema(path string) (*Schema, error) {
	r, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return NewJSONSchema(r)
}

func (sc *Schema) ToJSONFile(path string) error {
	if path == "" {
		path = "resources/schema/" + sc.DatabaseName + "_" + sc.Table.Name + ".json"
	}
	w, err := os.OpenFile(path, os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	return sc.ToJSON(w)
}

func (sc *Schema) ToJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	return enc.Encode(sc)
}
