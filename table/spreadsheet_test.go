package table

import (
	"testing"

	"github.com/Mitu217/tamate/table/schema"
)

func TestSpreadsheetGetRows(t *testing.T) {
	sc, err := schema.NewSchemaFromRow("ClassData", []string{"id", "name", "age"})
	if err != nil {
		t.Fatal(err)
	}

	conf := &SpreadsheetTableConfig{
		SpreadSheetsID: "1_QJnlgP9WI27KdJbWjFS8so1gjhXpEHizAQ5melyXEs",
		SheetName:      "ClassData",
		Range:          "A1:XX",
	}
	tbl, err := NewSpreadsheet(sc, conf)
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
