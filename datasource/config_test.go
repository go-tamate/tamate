package datasource

import (
	"strings"
	"testing"
)

func TestNewConfigFromJSON(t *testing.T) {
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
