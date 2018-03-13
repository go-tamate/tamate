package datasource

import (
	"strings"
	"testing"
)

func TestNewSpreadSheetsDataSource(t *testing.T) {
	conf := &SpreadSheetsDatasourceConfig{
		Type:           "spreadsheets",
		SpreadSheetsID: "1_QJnlgP9WI27KdJbWjFS8so1gjhXpEHizAQ5melyXEs",
		SheetName:      "ClassData",
		Range:          "A1:XX",
	}

	ds, err := NewSpreadSheetsDataSource(conf)
	if err != nil {
		t.Fatal(err)
	}

	rows, err := ds.GetRows()
	if err != nil {
		t.Fatal(err)
	}

	for _, row := range rows.Values {
		t.Logf(strings.Join(row, ","))
	}
}
