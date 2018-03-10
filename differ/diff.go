package differ

import (
	"github.com/Mitu217/tamate/datasource"
)

// DiffColumns :
type DiffColumns struct {
	Add    []string
	Delete []string
}

// DiffRows :
type DiffRows struct {
	Add    *datasource.Rows
	Modify *datasource.Rows
	Delete *datasource.Rows
}

// IsDiff :
func (dc *DiffColumns) IsDiff() bool {
	if len(dc.Add) != 0 {
		return true
	}
	if len(dc.Delete) != 0 {
		return true
	}
	return false
}

// IsDiff :
func (dr *DiffRows) IsDiff() bool {
	if len(dr.Add.Values) != 0 {
		return true
	}
	if len(dr.Delete.Values) != 0 {
		return true
	}
	if len(dr.Modify.Values) != 0 {
		return true
	}
	return false
}
