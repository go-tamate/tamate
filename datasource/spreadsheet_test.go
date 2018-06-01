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

func spreadsheetTestCase(t *testing.T, fun func(*SpreadsheetDatasource) error) {
	ctx := context.Background()
	client, err := getSpreadsheetClient(ctx, t)
	if err != nil {
		t.Fatal(err)
	}

	ds, err := NewSpreadsheetDatasource(client, spreadsheetID, "A1:Z100", 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := fun(ds); err != nil {
		t.Fatal(err)
	}
}

func getSpreadsheetClient(ctx context.Context, t *testing.T) (*http.Client, error) {
	encJsonKey := os.Getenv("TAMATE_SPREADSHEET_SERVICE_ACCOUNT_JSON_BASE64")
	if encJsonKey == "" {
		t.Skip("env: TAMATE_SPREADSHEET_SERVICE_ACCOUNT_JSON_BASE64 not set")
	}

	jsonKey, err := base64.StdEncoding.DecodeString(encJsonKey)
	if err != nil {
		return nil, err
	}
	return newServiceAccountClient(ctx, jsonKey)
}

func TestSpreadsheet_Get(t *testing.T) {
	spreadsheetTestCase(t, func(ds *SpreadsheetDatasource) error {
		ctx := context.Background()
		sc, err := ds.GetSchema(ctx, tableName)
		if err != nil {
			return err
		}

		t.Logf("Schema: %+v", sc)

		rows, err := ds.GetRows(ctx, sc)
		if err != nil {
			return err
		}

		for _, row := range rows {
			t.Log(row)
		}
		return nil
	})
}
