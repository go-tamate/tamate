package datasource

import (
	"encoding/json"
	"golang.org/x/oauth2"
	"os"
	"testing"
)

func getTokenFromFile(path string) (*oauth2.Token, error) {
	f, err := os.Open(path)
	defer f.Close()
	if err != nil {
		return nil, err
	}
	tok := &oauth2.Token{}
	err = json.NewDecoder(f).Decode(tok)
	return tok, err
}

const (
	spreadsheetID = "1_QJnlgP9WI27KdJbWjFS8so1gjhXpEHizAQ5melyXEs"
	tableName     = "ClassData"
)

func TestSpreadsheet_Get(t *testing.T) {
	oauthTokenPath := os.Getenv("TAMATE_SPREADSHEET_OAUTH_TOKEN_PATH")
	if oauthTokenPath == "" {
		t.Skip("env: TAMATE_SPREADSHEET_OAUTH_TOKEN_PATH not set")
	}
	tok, err := getTokenFromFile(oauthTokenPath)
	if err != nil {
		t.Fatal(err)
	}

	h, err := NewSpreadsheetDatasource(tok, spreadsheetID, "A1:C100", 0)
	if err != nil {
		t.Fatal(err)
	}

	sc, err := h.GetSchema(tableName)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("PK: %+v", sc.PrimaryKey)
	t.Logf("Columns: %+v", sc.GetColumnNames())

	rows, err := h.GetRows(sc)
	if err != nil {
		t.Fatal(err)
	}

	for _, row := range rows.Values {
		t.Log(row)
	}
}
