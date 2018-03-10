package config

import (
	"encoding/json"
	"io/ioutil"
)

// CSVConfig :
type CSVConfig struct {
	Path string
}

// Output :
func (c *CSVConfig) Output(path string) error {
	jsonBytes, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(path, jsonBytes, 0644)
}
