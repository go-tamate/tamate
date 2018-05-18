package datasource

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"golang.org/x/oauth2"
	"google.golang.org/api/sheets/v4"
)

const (
	// ClientID is use for authentication
	ClientID = "1053404748146-rj1p0vpl91q8a1t3hq1ak095nm158bfb.apps.googleusercontent.com"
	// ClientSecret is use for authentication
	ClientSecret = "X8nsx4H33ln0dPFDw8wNEzLp"
)

type SpreadsheetDatasource struct {
	Token          oauth2.Token `json:"token"`
	SpreadsheetID  string       `json:"spreadsheet_id"`
	Ranges         string       `json:"ranges"`
	ColumnRowIndex int          `json:"column_row_index"`
	sheetService   *sheets.Service
}

func NewSpreadsheetDatasource(spreadsheetID string, ranges string, columnRowIndex int) (*SpreadsheetDatasource, error) {
	return &SpreadsheetDatasource{
		SpreadsheetID:  spreadsheetID,
		Ranges:         ranges,
		ColumnRowIndex: columnRowIndex,
	}, nil
}

func (h *SpreadsheetDatasource) Open() error {
	if h.sheetService == nil {
		client, err := h.getHTTPClient()
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
func (h *SpreadsheetDatasource) Close() error {
	if h.sheetService != nil {
		h.sheetService = nil
	}
	return nil
}

// GetSchemas is get all schemas method
func (h *SpreadsheetDatasource) GetSchemas() ([]*Schema, error) {
	var schemas []*Schema
	spreadsheet, err := h.sheetService.Spreadsheets.Get(h.SpreadsheetID).Do()
	if err != nil {
		return nil, err
	}
	for _, sheet := range spreadsheet.Sheets {
		sheetName := sheet.Properties.Title
		schema, err := h.GetSchema(sheetName)
		if err != nil {
			return nil, err
		}
		// when not define schema row
		if schema == nil {
			continue
		}
		schemas = append(schemas, schema)
	}
	return schemas, nil
}

// GetSchema is get schema method
func (h *SpreadsheetDatasource) GetSchema(name string) (*Schema, error) {

	var schema *Schema
	if h.ColumnRowIndex > 0 {
		readRange := name + "!" + h.Ranges
		response, err := h.sheetService.Spreadsheets.Values.Get(h.SpreadsheetID, readRange).Do()
		if err != nil {
			return nil, err
		}
		schema.Name = name
		for i, row := range response.Values {
			if i != h.ColumnRowIndex-1 {
				continue
			}
			columns := make([]*Column, len(row))
			for i := range row {
				columns[i] = &Column{
					Name: row[i].(string),
					Type: "string",
				}
			}
			schema.Columns = columns
			return schema, nil
		}
	}
	// @todo Correct ret val
	return nil, nil
}

// SetSchema is set schema method
func (h *SpreadsheetDatasource) SetSchema(schema *Schema) error {
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
func (h *SpreadsheetDatasource) GetRows(schema *Schema) (*Rows, error) {
	readRange := schema.Name + "!" + h.Ranges
	response, err := h.sheetService.Spreadsheets.Values.Get(h.SpreadsheetID, readRange).Do()
	if err != nil {
		return nil, err
	}
	var values [][]string
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
func (h *SpreadsheetDatasource) SetRows(schema *Schema, rows *Rows) error {
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

func (h *SpreadsheetDatasource) getHTTPClient() (*http.Client, error) {
	if !h.Token.Valid() {
		return nil, errors.New("not authorization")
	}
	return h.getClient(&oauth2.Config{
		ClientID:     ClientID,
		ClientSecret: ClientSecret,
		Scopes:       []string{"https://www.googleapis.com/auth/spreadsheets"},
		Endpoint: oauth2.Endpoint{
			AuthURL:  "https://accounts.google.com/o/oauth2/v2/auth",
			TokenURL: "https://www.googleapis.com/oauth2/v4/token",
		},
	})
}

func (h *SpreadsheetDatasource) getClient(config *oauth2.Config) (*http.Client, error) {
	return config.Client(context.Background(), &h.Token), nil
}
