package config

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io/ioutil"
)

// BaseConfig :
type BaseConfig struct {
	ConfigType string `json:"config_type"`
}

// Config :
type Config interface{}

// OutputJSON :
func OutputJSON(config Config, jsonPath string) (string, error) {
	if jsonPath == "" {
		hashedFileNameBytes := sha256.Sum256([]byte(jsonPath))
		jsonPath = hex.EncodeToString(hashedFileNameBytes[:]) + ".json"
	}
	jsonBytes, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		return "", err
	}
	return jsonPath, ioutil.WriteFile(jsonPath, jsonBytes, 0644)
}
