package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io/ioutil"
)

// CSVConfig :
type CSVConfig struct {
	Path string
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
