package schema

import (
	"encoding/json"
	"io/ioutil"
)

type Schema struct {
	Name        string   `json:"name"`
	Description string   `json:"description"`
	Table       Table    `json:"table"`
	Properties  []Column `json:"properties"`
}

type Table struct {
	Name       string   `json:"name"`
	PrimaryKey string   `json:"primary_key"`
	UniqueKey  []string `json:"unique_key"`
}

type Column struct {
	Name          string `json:"name"`
	Type          string `json:"type"`
	NotNull       bool   `json:"not_null"`
	AutoIncrement bool   `json:"auto_increment"`
}

func LoadSchema(schemaPath string) Schema {
	file, err := ioutil.ReadFile(schemaPath)
	if err != nil {
		panic(err)
	}
	var schema Schema
	json.Unmarshal(file, &schema)
	return schema
}
