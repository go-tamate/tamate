package datasource

import (
	"context"
	"encoding/base64"
	"net/http"
	"os"
	"testing"

	"golang.org/x/oauth2/google"
)

const (
	TestSpreadsheetColumnRowIndex = 0
)

const (
	TestSpreadsheetIndexID = iota
	TestSpreadsheetIndexName
	TestSpreadsheetIndexAge
)

func getSpreadsheetTestData() [][]interface{} {
	return [][]interface{}{
		[]interface{}{"(id)", "name", "age"},
		[]interface{}{"1", "hana", "16"},
		[]interface{}{"2", "tamate", "15"},
		[]interface{}{"3", "kamuri", "15"},
		[]interface{}{"4", "eiko", "15"},
	}
}

type TestSpreadsheetService struct{}

func (s *TestSpreadsheetService) Get(ctx context.Context, spreadsheetID string, sheetName string) ([][]interface{}, error) {
	return getSpreadsheetTestData(), nil
}

func setupSpreadsheetDatasourceTest(t *testing.T) (func(), error) {
	encJsonKey := os.Getenv("TAMATE_SPREADSHEET_SERVICE_ACCOUNT_JSON_BASE64")
	if encJsonKey != "" {
		//t.Skip("env: TAMATE_SPREADSHEET_SERVICE_ACCOUNT_JSON_BASE64 not set")
	}
	return func() {}, nil
}

func TestSpreadsheet_Get(t *testing.T) {
	TearDown, err := setupSpreadsheetDatasourceTest(t)
	if err != nil {
		t.Fatal(err)
	}
	defer TearDown()

	ctx := context.Background()

	spreadsheetValues := getSpreadsheetTestData()

	service := &TestSpreadsheetService{}
	ds, err := NewSpreadsheetDatasource(service, "", TestSpreadsheetColumnRowIndex)

	sc, err := ds.GetSchema(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	if sc.Columns[TestSpreadsheetIndexID].Name != "id" {
		t.Fatalf("sc.Columns[%d].Name must be %+v, but actual: %+v", TestSpreadsheetIndexID, "id", sc.Columns[TestSpreadsheetIndexID].Name)
	}
	if sc.Columns[TestSpreadsheetIndexName].Name != "name" {
		t.Fatalf("sc.Columns[%d].Name must be %+v, but actual: %+v", TestSpreadsheetIndexName, "name", sc.Columns[TestSpreadsheetIndexName].Name)
	}
	if sc.Columns[TestSpreadsheetIndexAge].Name != "age" {
		t.Fatalf("sc.Columns[%d].Name must be %+v, but actual: %+v", TestSpreadsheetIndexAge, "age", sc.Columns[TestSpreadsheetIndexAge].Name)
	}

	rows, err := ds.GetRows(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}

	for i, row := range rows {
		index := i
		if index >= ds.ColumnRowIndex {
			index++
		}
		if row.Values["id"].Value != spreadsheetValues[index][TestSpreadsheetIndexID] {
			t.Fatalf("rows[%d].Values['id'] must be %+v, but actual: %+v", i, row.Values["id"].Value, spreadsheetValues[index][TestSpreadsheetIndexID])
		}
		if row.Values["name"].Value != spreadsheetValues[index][TestSpreadsheetIndexName] {
			t.Fatalf("rows[%d].Values['name'] must be %+v, but actual: %+v", i, row.Values["name"].Value, spreadsheetValues[index][TestSpreadsheetIndexName])
		}
		if row.Values["age"].Value != spreadsheetValues[index][TestSpreadsheetIndexAge] {
			t.Fatalf("rows[%d].Values['age'] must be %+v, but actual: %+v", i, row.Values["age"].Value, spreadsheetValues[index][TestSpreadsheetIndexAge])
		}
	}
}

func TestSpreadsheet_Connect(t *testing.T) {
	ctx := context.Background()
	client, err := getSpreadsheetClient(ctx, t)
	if err != nil {
		t.Fatal(err)
	}

	sheetId := os.Getenv("TAMATE_TEST_SPREADSHEET_SHEET_ID")
	sheetName := os.Getenv("TAMATE_TEST_SPREADSHEET_SHEET_NAME")
	if sheetId == "" {
		t.Skip("env: TAMATE_TEST_SPREADSHEET_SHEET_ID not set")
	}
	if sheetId == "" {
		t.Skip("env: TAMATE_TEST_SPREADSHEET_SHEET_NAME not set")
	}
	ds, err := NewGoogleSpreadsheetDatasource(client, sheetId, TestSpreadsheetColumnRowIndex)
	if err != nil {
		t.Fatal(err)
	}
	sc, err := ds.GetSchema(ctx, sheetName)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := ds.GetRows(ctx, sc); err != nil {
		t.Fatal(err)
	}
}

func newServiceAccountClient(ctx context.Context, jsonKey []byte) (*http.Client, error) {
	conf, err := google.JWTConfigFromJSON(jsonKey, "https://www.googleapis.com/auth/spreadsheets")
	if err != nil {
		return nil, err
	}
	return conf.Client(ctx), nil
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
