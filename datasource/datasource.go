package datasource

import (
	"github.com/Mitu217/tamate/schema"
)

// Rows :
type Rows struct {
	Columns []string
	Values  [][]string
}

// DataSource :
type DataSource interface {
	GetSchema() (schema.Schema, error)
	GetRows() (*Rows, error)
	SetRows(*Rows) error
}
