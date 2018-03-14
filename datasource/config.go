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

func ConfigToJSONFile(path string, conf interface{}) error {
	w, err := os.OpenFile(path, os.O_CREATE, 0644)
	defer w.Close()
	if err != nil {
		return err
	}
	return ConfigToJSON(w, conf)
}

func ConfigToJSON(w io.Writer, conf interface{}) error {
	return json.NewEncoder(w).Encode(conf)
}
