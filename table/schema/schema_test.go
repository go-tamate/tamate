package schema

import (
	"bytes"
	"encoding/json"
	"log"
	"reflect"
	"testing"
)

func TestSchemaToJSON(t *testing.T) {
	sc := &Schema{
		DatabaseName: "testdb",
		Description:  "testdescription",
		Table:        Table{Name: "testtable", PrimaryKey: "id"},
		Columns: []Column{
			{"id", "int", true, true},
			{"name", "text", true, false},
		},
	}

	var b bytes.Buffer
	if err := sc.ToJSON(&b); err != nil {
		log.Fatalln(err)
	}

	sc2 := &Schema{}
	if err := json.Unmarshal(b.Bytes(), sc2); err != nil {
		log.Fatalln(err)
	}

	if !reflect.DeepEqual(sc, sc2) {
		log.Fatalf("expected %+v, actual %+v", sc, sc2)
	}
}
