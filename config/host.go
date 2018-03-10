package config

import (
	"encoding/json"
	"io/ioutil"
)

// HostConfig :
type HostConfig struct {
	DriverName string `json:"driver_name"`
	Host       string `json:"host"`
	Port       int    `json:"port"`
	User       string `json:"user"`
	Password   string `json:"password"`
}

// Output :
func (c *HostConfig) Output(path string) error {
	// Set default path and default file name.
	if path == "" {
		path = "resources/host/" + c.DriverName + "_" + c.Host + "_" + "json"
	}

	jsonBytes, err := json.MarshalIndent(c, "", "  ")
	if err != nil {
		return err
	}
	return ioutil.WriteFile(path, jsonBytes, 0644)
}
