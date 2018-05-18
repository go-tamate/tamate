package datasource

import (
	"context"
	"encoding/base64"
	"golang.org/x/oauth2/google"
	"net/http"
	"os"
	"testing"
)

func newServiceAccountClient(ctx context.Context, jsonKey []byte) (*http.Client, error) {
	conf, err := google.JWTConfigFromJSON(jsonKey, "https://www.googleapis.com/auth/spreadsheets")
	if err != nil {
		return nil, err
	}
	return conf.Client(ctx), nil
}

const (
	spreadsheetID = "1_QJnlgP9WI27KdJbWjFS8so1gjhXpEHizAQ5melyXEs"
	tableName     = "ClassData"
)

func TestSpreadsheet_Get(t *testing.T) {
	encJsonKey := os.Getenv("TAMATE_SPREADSHEET_SERVICE_ACCOUNT_JSON_BASE64")
	if encJsonKey == "" {
		t.Skip("env: TAMATE_SPREADSHEET_SERVICE_ACCOUNT_JSON_BASE64 not set")
	}

	jsonKey, err := base64.StdEncoding.DecodeString(encJsonKey)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	tok, err := newServiceAccountClient(ctx, jsonKey)
	if err != nil {
		t.Fatal(err)
	}

	h, err := NewSpreadsheetDatasource(tok, spreadsheetID, "A1:C100", 0)
	if err != nil {
		t.Fatal(err)
	}

	sc, err := h.GetSchema(ctx, tableName)
	if err != nil {
		t.Fatal(err)
	}

	t.Logf("PK: %+v", sc.PrimaryKey)
	t.Logf("Columns: %+v", sc.GetColumnNames())

	rows, err := h.GetRows(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}

	for _, row := range rows.Values {
		t.Log(row)
	}
}
