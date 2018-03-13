package datasource

import (
	"strings"
	"testing"
	"bytes"
)

func TestCSVNewConfigFromJSON(t *testing.T) {
	j := `{"type": "csv", "path": "/path/to/data.csv"}`
	jr := strings.NewReader(j)

	var conf CSVDatasourceConfig
	if err := NewConfigFromJSON(jr, &conf); err != nil {
		t.Fatal(err)
	}
	if conf.Type != "csv" {
		t.Fatalf("type must be csv, but %s", conf.Type)
	}
	if conf.Path != "/path/to/data.csv" {
		t.Fatalf("path must be /path/to/data.csv, but %s", conf.Path)
	}
}

func TestCSVConfigToJSON(t *testing.T) {
	conf := &CSVDatasourceConfig{Type: "csv", Path: "/path/to/data.csv"}
	var b bytes.Buffer
	if err := ConfigToJSON(&b, conf); err != nil {
		t.Fatal(err)
	}

	expected := `{"type":"csv","path":"/path/to/data.csv"}` + "\n"
	actual := b.String()
	if actual != expected {
		t.Fatalf("expected %s, but actual %s", expected, actual)
	}
}