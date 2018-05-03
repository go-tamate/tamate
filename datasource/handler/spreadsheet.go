package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/sheets/v4"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strings"
)

const (
	// CredentialJSON is use access spreadsheet
	CredentialJSON = `{"installed":{"client_id":"91207976446-6jhqnhlqitbv60fskj0uulq3hf2iil1t.apps.googleusercontent.com","project_id":"nifty-inkwell-197212","auth_uri":"https://accounts.google.com/o/oauth2/auth","token_uri":"https://accounts.google.com/o/oauth2/token","auth_provider_x509_cert_url":"https://www.googleapis.com/oauth2/v1/certs","client_secret":"Baxr8JY1P5WD1ggkn1aRptzR","redirect_uris":["urn:ietf:wg:oauth:2.0:oob","http://localhost"]}}`
)

// SpreadsheetHandler is handler struct of csv
type SpreadsheetHandler struct {
	SpreadsheetID  string `json:"spreadsheet_id"`
	Ranges         string `json:"ranges"`
	ColumnRowIndex int    `json:"column_row_index"`
	sheetService   *sheets.Service
}

// NewSpreadsheetHandler is create SpreadsheetHandler instance method
func NewSpreadsheetHandler(spreadsheetID string, ranges string, columnRowIndex int) (*SpreadsheetHandler, error) {
	return &SpreadsheetHandler{
		SpreadsheetID:  spreadsheetID,
		Ranges:         ranges,
		ColumnRowIndex: columnRowIndex,
	}, nil
}

// Open is call by datasource when create instance
func (h *SpreadsheetHandler) Open() error {
	if h.sheetService == nil {
		client, err := httpClient()
		if err != nil {
			return err
		}
		sheetService, err := sheets.New(client)
		if err != nil {
			return err
		}
		h.sheetService = sheetService
	}
	return nil
}

// Close is call by datasource when free instance
func (h *SpreadsheetHandler) Close() error {
	if h.sheetService != nil {
		h.sheetService = nil
	}
	return nil
}

// GetSchemas is get all schemas method
func (h *SpreadsheetHandler) GetSchemas() (*[]Schema, error) {
	schemas := make([]Schema, 0)
	spreadsheet, err := h.sheetService.Spreadsheets.Get(h.SpreadsheetID).Do()
	if err != nil {
		return nil, err
	}
	for _, sheet := range spreadsheet.Sheets {
		sheetName := sheet.Properties.Title
		schema := &Schema{
			Name: sheetName,
		}
		err := h.GetSchema(schema)
		if err != nil {
			return nil, err
		}
		// when not define schema row
		if schema == nil {
			continue
		}
		schemas = append(schemas, *schema)
	}
	return &schemas, nil
}

// GetSchema is get schema method
func (h *SpreadsheetHandler) GetSchema(schema *Schema) error {
	if h.ColumnRowIndex > 0 {
		readRange := schema.Name + "!" + h.Ranges
		response, err := h.sheetService.Spreadsheets.Values.Get(h.SpreadsheetID, readRange).Do()
		if err != nil {
			return err
		}
		for i, row := range response.Values {
			if i != h.ColumnRowIndex-1 {
				continue
			}
			columns := make([]Column, len(row))
			for i := range row {
				columns[i] = Column{
					Name: row[i].(string),
					Type: "string",
				}
			}
			schema.Columns = columns
			return nil
		}
	}
	return nil
}

// SetSchema is set schema method
func (h *SpreadsheetHandler) SetSchema(schema *Schema) error {
	if h.ColumnRowIndex > 0 {
		schemaValue := make([]interface{}, len(schema.Columns))
		for i := range schema.Columns {
			schemaValue[i] = schema.Columns[i].Name
		}
		valueRange := &sheets.ValueRange{
			MajorDimension: "ROWS",
			Values:         [][]interface{}{schemaValue},
		}

		// FIXME:
		writeRange := schema.Name + "!" + fmt.Sprintf("A%d:XX", h.ColumnRowIndex)
		if _, err := h.sheetService.Spreadsheets.Values.Update(h.SpreadsheetID, writeRange, valueRange).ValueInputOption("USER_ENTERED").Do(); err != nil {
			return err
		}
	}
	return nil
}

// GetRows is get rows method
func (h *SpreadsheetHandler) GetRows(schema *Schema) (*Rows, error) {
	readRange := schema.Name + "!" + h.Ranges
	response, err := h.sheetService.Spreadsheets.Values.Get(h.SpreadsheetID, readRange).Do()
	if err != nil {
		return nil, err
	}
	values := [][]string{}
	for i, row := range response.Values {
		if i == h.ColumnRowIndex-1 {
			continue
		}
		value := make([]string, len(row))
		for i := range row {
			// FIXME: Datatime
			value[i] = row[i].(string)
		}
		values = append(values, value)
	}
	return &Rows{
		Values: values,
	}, nil
}

// SetRows is set rows method
func (h *SpreadsheetHandler) SetRows(schema *Schema, rows *Rows) error {
	rowsValues := make([][]interface{}, 0)
	for i, value := range rows.Values {
		if i == h.ColumnRowIndex-1 {
			rowsValues = append(rowsValues, make([]interface{}, 0))
		}
		row := make([]interface{}, len(value))
		for j := range value {
			row[j] = value[j]
		}
		rowsValues = append(rowsValues, row)
	}
	valueRange := &sheets.ValueRange{
		MajorDimension: "ROWS",
		Values:         rowsValues,
	}

	// FIXME:
	writeRange := schema.Name + "!" + fmt.Sprintf("A%d:XX", h.ColumnRowIndex)
	if _, err := h.sheetService.Spreadsheets.Values.Update(h.SpreadsheetID, writeRange, valueRange).ValueInputOption("USER_ENTERED").Do(); err != nil {
		return err
	}
	return nil
}

func httpClient() (*http.Client, error) {
	bytes, err := ioutil.ReadAll(strings.NewReader(CredentialJSON))
	if err != nil {
		return nil, err
	}
	config, err := google.ConfigFromJSON(bytes, "https://www.googleapis.com/auth/spreadsheets")
	if err != nil {
		return nil, err
	}
	return getClient(config), nil
}

func getClient(config *oauth2.Config) *http.Client {
	tokFile := "token.json"
	tok, err := tokenFromFile(tokFile)
	if err != nil {
		tok = getTokenFromWeb(config)
		saveToken(tokFile, tok)
	}
	return config.Client(context.Background(), tok)
}

// Retrieves a token from a local file.
func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	defer f.Close()
	if err != nil {
		return nil, err
	}
	tok := &oauth2.Token{}
	err = json.NewDecoder(f).Decode(tok)
	return tok, err
}

// Saves a token to a file path.
func saveToken(path string, token *oauth2.Token) {
	fmt.Printf("Saving credential file to: %s\n", path)
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	defer f.Close()
	if err != nil {
		log.Fatalf("Unable to cache oauth token: %v", err)
	}
	json.NewEncoder(f).Encode(token)
}

// Request a token from the web, then returns the retrieved token.
func getTokenFromWeb(config *oauth2.Config) *oauth2.Token {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	fmt.Printf("Go to the following link in your browser then type the "+
		"authorization code: \n%v\n", authURL)

	var authCode string
	if _, err := fmt.Scan(&authCode); err != nil {
		log.Fatalf("Unable to read authorization code: %v", err)
	}

	tok, err := config.Exchange(oauth2.NoContext, authCode)
	if err != nil {
		log.Fatalf("Unable to retrieve token from web: %v", err)
	}
	return tok
}
