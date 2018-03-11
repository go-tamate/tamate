package datasource

import (
	"encoding/json"
	"io"
	"os"
)

func NewConfigFromJSON(r io.Reader, v interface{}) error {
	return json.NewDecoder(r).Decode(v)
}

func NewConfigFromJSONFile(path string, v interface{}) error {
	r, err := os.Open(path)
	defer r.Close()
	if err != nil {
		return err
	}
	return NewConfigFromJSON(r, v)
}
