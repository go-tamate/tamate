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
		return "CSV"
	case SQL:
		return "SQL"
	case Spreadsheet:
		return "Spreadsheet"
	default:
		return "Unknown"
	}
}
