package datasource

import (
	"path"
	"runtime"
	"strings"
	"testing"
)

func TestNewSpreadSheetsDataSource(t *testing.T) {
	_, fi, _, _ := runtime.Caller(0)
	cdir := path.Dir(fi)
	conf := &SpreadSheetsDatasourceConfig{
		Type:                "spreadsheets",
		CredentialsJSONPath: cdir + "/../resources/spreadsheets/client_secret.json",
		SpreadSheetsID:      "1_QJnlgP9WI27KdJbWjFS8so1gjhXpEHizAQ5melyXEs",
		SheetName:           "ClassData",
		Range:               "A1:XX",
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
