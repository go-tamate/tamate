package server

import (
	"encoding/json"
	"io"
	"os"
)

type Server struct {
	DriverName string `json:"driver_name"`
	Host       string `json:"host"`
	Port       int    `json:"port"`
	User       string `json:"user"`
	Password   string `json:"password"`
}

func NewJsonServer(r io.Reader) (*Server, error) {
	var sv *Server
	if err := json.NewDecoder(r).Decode(&sv); err != nil {
		return nil, err
	}
	return sv, nil
}

func NewJsonFileServer(path string) (*Server, error) {
	r, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return NewJsonServer(r)
}
