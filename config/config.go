package config

// BaseConfig :
type BaseConfig struct {
	Type string `json:"type"`
}

// Config :
type Config interface {
	Output(path string) (*string, error)
}
