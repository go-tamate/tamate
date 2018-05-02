package table

import (
	"bytes"
	"reflect"
	"testing"

	"github.com/Mitu217/tamate/table/schema"
)

func TestToJSON(t *testing.T) {
	conf := &CSVConfig{
		Path: "/path/to/test.csv",
	}
	sc, err := schema.NewSchemaFromRow("test", []string{"id", "name", "age"})
	if err != nil {
		t.Fatal(err)
	}
	tbl, err := NewCSV(sc, conf)
	if err != nil {
		t.Fatal(err)
	}

	var b bytes.Buffer
	if err := ToJSON(tbl, &b); err != nil {
		t.Fatal(err)
	}
	t.Log(string(b.Bytes()))

	tbl2, err := FromJSON(&b)
	if err != nil {
		t.Fatal(err)
	}
	typeName := reflect.TypeOf(tbl2).String()
	expected := "*table.CSVTable"
	if typeName != expected {
		t.Fatalf("typeof tbl2 must be %s, actual: %s", expected, typeName)
	}
}
