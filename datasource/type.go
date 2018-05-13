package datasource

// Type defines datasource type
type Type int

// table types
const (
	CSV Type = iota
	SQL
	Spreadsheet
)

func (t Type) String() string {
	switch t {
	case CSV:
		return "csv"
	case SQL:
		return "sql"
	case Spreadsheet:
		return "spreadsheet"
	default:
		return "unknown"
	}
}
