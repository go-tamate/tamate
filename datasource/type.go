package datasource

// TableType defines datasource table type
type TableType int

// table types
const (
	CSV TableType = iota
	SQL
	SpreadSheet
)

func (tt TableType) String() string {
	switch tt {
	case CSV:
		return "CSVTable"
	case SQL:
		return "SQLtable"
	case SpreadSheet:
		return "SpreadSheetTable"
	default:
		return ""
	}
}
