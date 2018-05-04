package differ

import (
	"github.com/Mitu217/tamate/datasource/handler"
)

// DiffColumns :
type DiffColumns struct {
	Add    []handler.Column
	Modify []handler.Column
	Delete []handler.Column
}

// DiffRows :
type DiffRows struct {
	Add    *handler.Rows
	Modify *handler.Rows
	Delete *handler.Rows
}

// IsDiff :
func (dc *DiffColumns) IsDiff() bool {
	if len(dc.Add) != 0 {
		return true
	}
	if len(dc.Delete) != 0 {
		return true
	}
	if len(dc.Modify) != 0 {
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
