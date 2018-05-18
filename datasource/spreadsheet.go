package datasource

import (
	"fmt"

	"context"
	"errors"
	"golang.org/x/oauth2"
	"google.golang.org/api/sheets/v4"
)

type SpreadsheetDatasource struct {
	SpreadsheetID  string `json:"spreadsheet_id"`
	Ranges         string `json:"ranges"`
	ColumnRowIndex int    `json:"column_row_index"`
	sheetService   *sheets.Service
}

func NewSpreadsheetDatasource(token *oauth2.Token, spreadsheetID string, ranges string, columnRowIndex int) (*SpreadsheetDatasource, error) {
	config := oauth2.Config{}
	client := config.Client(context.Background(), token)
	ss, err := sheets.New(client)
	if err != nil {
		return nil, err
	}
	return &SpreadsheetDatasource{
		SpreadsheetID:  spreadsheetID,
		Ranges:         ranges,
		ColumnRowIndex: columnRowIndex,
		sheetService:   ss,
	}, nil
}

func (h *SpreadsheetDatasource) GetAllSchema() ([]*Schema, error) {
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

func (h *SpreadsheetDatasource) GetSchema(name string) (*Schema, error) {
	readRange := name + "!" + h.Ranges
	response, err := h.sheetService.Spreadsheets.Values.Get(h.SpreadsheetID, readRange).Do()
	if err != nil {
		return nil, err
	}
	for i, row := range response.Values {
		if i == h.ColumnRowIndex {
			columns := make([]*Column, len(row))
			for i := range row {
				columns[i] = &Column{
					Name: row[i].(string),
					Type: "string",
				}
			}
			pk, err := choosePrimaryKey(columns)
			if err != nil {
				return nil, err
			}
			return &Schema{
				Columns:    columns,
				PrimaryKey: pk,
			}, nil
		}
	}
	return nil, errors.New("could not find column row")
}

func choosePrimaryKey(columns []*Column) (*PrimaryKey, error) {
	// TODO: primary key choosing algorightm for spreadsheet
	return &PrimaryKey{
		ColumnNames: []string{columns[0].Name},
	}, nil
}

// SetSchema is set schema method
func (h *SpreadsheetDatasource) SetSchema(schema *Schema) error {
	schemaValue := make([]interface{}, len(schema.Columns))
	for i := range schema.Columns {
		schemaValue[i] = schema.Columns[i].Name
	}
	valueRange := &sheets.ValueRange{
		MajorDimension: "ROWS",
		Values:         [][]interface{}{schemaValue},
	}

	// FIXME:
	writeRange := schema.Name + "!" + fmt.Sprintf("A%d:XX", h.ColumnRowIndex+1)
	if _, err := h.sheetService.Spreadsheets.Values.Update(h.SpreadsheetID, writeRange, valueRange).ValueInputOption("USER_ENTERED").Do(); err != nil {
		return err
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
		if i == h.ColumnRowIndex {
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
		if i == h.ColumnRowIndex {
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
	writeRange := schema.Name + "!" + fmt.Sprintf("A%d:XX", h.ColumnRowIndex+1)
	if _, err := h.sheetService.Spreadsheets.Values.Update(h.SpreadsheetID, writeRange, valueRange).ValueInputOption("USER_ENTERED").Do(); err != nil {
		return err
	}
	return nil
}
