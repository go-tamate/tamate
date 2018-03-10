package config

// Config :
type Config interface {
	Output(path string) (*string, error)
}
