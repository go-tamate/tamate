package config

import (
	"encoding/json"
	"io/ioutil"
	"os"
)

// ServerConfig :
type ServerConfig struct {
	DriverName string `json:"driver_name"`
	Host       string `json:"host"`
	Port       int    `json:"port"`
	User       string `json:"user"`
	Password   string `json:"password"`
}

// NewJSONServerConfig :
func NewJSONServerConfig(jsonPath string) (*ServerConfig, error) {
	var serverConfig *ServerConfig
	r, err := os.Open(jsonPath)
	if err != nil {
		return nil, err
	}

	if err := json.NewDecoder(r).Decode(&serverConfig); err != nil {
		return nil, err
	}
	return serverConfig, nil
}

// Output :
func (c *ServerConfig) Output(path string) (string, error) {
	// Set default path and default file name.
	if path == "" {
		path = "resources/config/server/" + c.DriverName + "_" + c.Host + ".json"
	}

	jsonBytes, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return "", err
	}
	return path, ioutil.WriteFile(path, jsonBytes, 0644)
}
