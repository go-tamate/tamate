package config

// Config :
type Config interface {
	Output(path string) error
}
