package datasource

import (
	"context"
	"encoding/base64"
	"golang.org/x/oauth2/google"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"
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
	testSpreadsheetRowCount  = 100
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

		actualRowCount := 0
		for _, row := range rows {
			if !strings.HasPrefix(row.Values["ID"].StringValue(), "ID") {
				t.Fatalf("ID must have prefix: ID, but actual: %+v.", row.Values["ID"].Value)
			}
			if !strings.HasPrefix(row.Values["StringTest"].StringValue(), "testString") {
				t.Fatalf("StringTest must have prefix: testString, but actual: %+v.", row.Values["StringTest"].Value)
			}
			if row.Values["AlwaysNullStringTest"].Value != nil {
				t.Fatalf("AlwaysNullStringTest must be nil, but actual: %+v.", row.Values["AlwaysNullStringTest"].Value)
			}
			if row.Values["IntTest"].Value != "123456" {
				t.Fatalf("IntTest value must be '123456', but actual: %+v.", row.Values["IntTest"].Value)
			}
			if _, err := time.Parse("2006/01/02", row.Values["DateTest"].StringValue()); err != nil {
				t.Fatalf("DateTest value must be yyyy-mm-dd format, but actual: %+v.", row.Values["DateTest"].Value)
			}
			if row.Values["Int64ArrayTest"].Value != "123,456,-789" {
				t.Fatalf("Int64ArrayTest must be '123,456,-789', but actual: %+v.", row.Values["Int64ArrayTest"].Value)
			}
			actualRowCount++
		}
		if actualRowCount != testSpreadsheetRowCount {
			t.Fatalf("spreadsheet rowCount must be %d, but actual: %d.", testSpreadsheetRowCount, actualRowCount)
		}
		return nil
	})
}
