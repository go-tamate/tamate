package datasource

import (
	"context"
	"fmt"
	"net/http"

	"google.golang.org/api/sheets/v4"
)

type SpreadsheetDatasource struct {
	SpreadsheetID  string `json:"spreadsheet_id"`
	ColumnRowIndex int    `json:"column_row_index"`
	sheetService   *sheets.Service
}

func NewSpreadsheetDatasource(client *http.Client, spreadsheetID string, columnRowIndex int) (*SpreadsheetDatasource, error) {
	ss, err := sheets.New(client)
	if err != nil {
		return nil, err
	}
	return &SpreadsheetDatasource{
		SpreadsheetID:  spreadsheetID,
		ColumnRowIndex: columnRowIndex,
		sheetService:   ss,
	}, nil
}

func (ds *SpreadsheetDatasource) GetAllSchema(ctx context.Context) ([]*Schema, error) {
	var schemas []*Schema
	spreadsheet, err := ds.sheetService.Spreadsheets.Get(ds.SpreadsheetID).Do()
	if err != nil {
		return nil, err
	}
	for _, sheet := range spreadsheet.Sheets {
		if sheet.Properties.Hidden {
			// ignore hidden sheet
			continue
		}
		sheetName := sheet.Properties.Title
		schema, err := ds.GetSchema(ctx, sheetName)
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

func (ds *SpreadsheetDatasource) GetSchema(ctx context.Context, name string) (*Schema, error) {
	readRange := name
	response, err := ds.sheetService.Spreadsheets.Values.Get(ds.SpreadsheetID, readRange).Do()
	if err != nil {
		return nil, err
	}
	for i, row := range response.Values {
		if i == ds.ColumnRowIndex {
			columns := make([]*Column, len(row))
			for i := range row {
				columns[i] = &Column{
					Name: row[i].(string),
					Type: ColumnTypeString,
				}
			}
			pk, err := choosePrimaryKey(columns)
			if err != nil {
				return nil, err
			}
			return &Schema{
				Name:       name,
				Columns:    columns,
				PrimaryKey: pk,
			}, nil
		}
	}
	return nil, nil
}

func choosePrimaryKey(columns []*Column) (*Key, error) {
	// TODO: primary key choosing algorightm for spreadsheet
	return &Key{
		KeyType:     KeyTypePrimary,
		ColumnNames: []string{columns[0].Name},
	}, nil
}

// SetSchema is set schema method
func (ds *SpreadsheetDatasource) SetSchema(ctx context.Context, schema *Schema) error {
	schemaValue := make([]interface{}, len(schema.Columns))
	for i := range schema.Columns {
		schemaValue[i] = schema.Columns[i].Name
	}
	valueRange := &sheets.ValueRange{
		MajorDimension: "ROWS",
		Values:         [][]interface{}{schemaValue},
	}

	writeRange := schema.Name
	if _, err := ds.sheetService.Spreadsheets.Values.Update(ds.SpreadsheetID, writeRange, valueRange).ValueInputOption("USER_ENTERED").Do(); err != nil {
		return err
	}
	return nil
}

// GetRows is get rows method
func (ds *SpreadsheetDatasource) GetRows(ctx context.Context, schema *Schema) ([]*Row, error) {
	readRange := schema.Name
	response, err := ds.sheetService.Spreadsheets.Values.Get(ds.SpreadsheetID, readRange).Do()
	if err != nil {
		return nil, err
	}
	var rows []*Row
	for i, sr := range response.Values {
		if i == ds.ColumnRowIndex {
			continue
		}
		rowValues := make(RowValues)
		rowValuesGroupByKey := make(GroupByKey)
		// TODO: correct order?
		for i, col := range schema.Columns {
			srt, ok := sr[i].(string)
			if !ok {
				return nil, fmt.Errorf("cannot convert spreadsheet value to string: %+v", sr[i])
			}
			// 空文字は NULL とみなす
			var cv *GenericColumnValue
			if srt == "" {
				cv = &GenericColumnValue{Column: col, Value: nil}
			} else {
				cv = &GenericColumnValue{Column: col, Value: srt}
			}
			rowValues[col.Name] = cv
			for _, name := range schema.PrimaryKey.ColumnNames {
				if name == col.Name {
					rowValuesGroupByKey[schema.PrimaryKey.String()] = append(rowValuesGroupByKey[schema.PrimaryKey.String()], cv)
				}
			}
		}
		rows = append(rows, &Row{rowValuesGroupByKey, rowValues})
	}
	return rows, nil
}

// SetRows is set rows method
func (ds *SpreadsheetDatasource) SetRows(ctx context.Context, schema *Schema, rows []*Row) error {
	sheetRows := make([][]interface{}, len(rows))
	colLen := len(schema.Columns)

	ri := 0
	for si := 0; si < len(rows)+1; si++ {
		sheetRow := make([]interface{}, colLen)
		for k, cn := range schema.GetColumnNames() {
			if si == ds.ColumnRowIndex {
				sheetRow[k] = cn
			} else {
				sheetRow[k] = rows[ri].Values[cn].StringValue()
				ri++
			}
		}
		sheetRows = append(sheetRows, sheetRow)
	}

	valueRange := &sheets.ValueRange{
		MajorDimension: "ROWS",
		Values:         sheetRows,
	}

	writeRange := schema.Name
	if _, err := ds.sheetService.Spreadsheets.Values.Update(ds.SpreadsheetID, writeRange, valueRange).ValueInputOption("USER_ENTERED").Do(); err != nil {
		return err
	}
	return nil
}
