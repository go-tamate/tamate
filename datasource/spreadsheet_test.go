package datasource

import (
	"context"
	"encoding/base64"
	"fmt"
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
	testSpreadsheetID        = "1txJ42ua9uGqJYFO8ann-_A9v_jdowCA1pr6pbchRFvY"
	testSpreadsheetTableName = "Test"
)

func spreadsheetTestCase(t *testing.T, fun func(*SpreadsheetDatasource) error) {
	ctx := context.Background()
	client, err := getSpreadsheetClient(ctx, t)
	if err != nil {
		t.Fatal(err)
	}

	ds, err := NewSpreadsheetDatasource(client, testSpreadsheetID, 0)
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
		sc, err := ds.GetSchema(ctx, testSpreadsheetTableName)
		if err != nil {
			return err
		}

		t.Logf("Schema: %+v", sc)

		rows, err := ds.GetRows(ctx, sc)
		if err != nil {
			return err
		}

		for _, row := range rows {
			if row.Values["AlwaysNullStringTest"].Value != nil {
				return fmt.Errorf("AlwaysNullString value must be null, but actual: %+v", row.Values["AlwaysNullStringTest"].Value)
			}
		}
		return nil
	})
}
