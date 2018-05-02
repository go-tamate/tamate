package config

// SpreadsheetTableConfig is table config struct of spreadsheet.
type SpreadsheetTableConfig struct {
	SpreadSheetsID string `json:"spreadsheets_id"`
	SheetName      string `json:"sheet_name"`
	Range          string `json:"range"`
}
