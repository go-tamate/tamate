package datasource

// Rows :
type Rows struct {
	Columns []string
	Values  [][]string
}

// DataSource :
type DataSource interface {
	GetRows() *Rows
	SetRows(*Rows)
	Output() error
}
