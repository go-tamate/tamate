package table

import (
	"github.com/Mitu217/tamate/table/schema"
	"strings"
	"testing"
)

func TestGetRows(t *testing.T) {
	sc, err := schema.NewSchemaFromRow("ClassData", []string{"id", "name", "age"})
	if err != nil {
		t.Fatal(err)
	}

	testData := `
1,hana,16
2,tamate,15
3,eiko,15
4,kamuri,15
`
	r := strings.NewReader(testData)
	tbl, err := newCSVFromReader(sc, r)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := tbl.GetRows()
	if err != nil {
		t.Fatal(err)
	}
	if len(rows.Values) != 4 {
		t.Fatal("table slow_start values count must be 4")
	}
	if rows.Values[3][1] != "kamuri" {
		t.Fatal("rows[3][1] must be kamuri")
	}
}
