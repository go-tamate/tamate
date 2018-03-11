package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io/ioutil"
	"os"
)

// CSVConfig :
type CSVConfig struct {
	Path string
}

// NewCSVConfig :
func NewCSVConfig(path string) *CSVConfig {
	config := &CSVConfig{
		Path: path,
	}
	return config
}

// NewJSONCSVConfig :
func NewJSONCSVConfig(jsonPath string) (*CSVConfig, error) {
	var config *CSVConfig
	r, err := os.Open(jsonPath)
	if err != nil {
		return nil, err
	}

	if err := json.NewDecoder(r).Decode(&config); err != nil {
		return nil, err
	}
	return config, nil
}

// Output :
func (c *CSVConfig) Output(path string) (string, error) {
	if path == "" {
		hashedFileNameBytes := sha256.Sum256([]byte(c.Path))
		path = "resources/config/csv/" + hex.EncodeToString(hashedFileNameBytes[:]) + ".json"
	}

	jsonBytes, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return "", err
	}
	return "", ioutil.WriteFile(path, jsonBytes, 0644)
}
