package table

import (
	"github.com/Mitu217/tamate/table/schema"
)

// Rows :
type Rows struct {
	Values [][]string
}

type Table interface {
	GetSchema() (*schema.Schema, error)
	GetRows() (*Rows, error)
}
