package differ

import "github.com/Mitu217/tamate/schema"

// Diff :
type Diff struct {
	Columns []schema.Column
	Add     [][]string
	Modify  [][]string
	Delete  [][]string
}

// IsExistDiff :
func (d *Diff) IsExistDiff() bool {
	if len(d.Add) != 0 {
		return true
	}
	if len(d.Delete) != 0 {
		return true
	}
	if len(d.Modify) != 0 {
		return true
	}
	return false
}
