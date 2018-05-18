package differ

import (
	"github.com/Mitu217/tamate/datasource"
)

// DiffColumns is add, modify and delete columns struct
type DiffColumns struct {
	Add    []ModifyColumnValues `json:"add"`
	Modify []ModifyColumnValues `json:"modify"`
	Delete []ModifyColumnValues `json:"delete"`
}

// ModifyColumnValues is modify column values struct between left and right
type ModifyColumnValues struct {
	Left  *datasource.Column `json:"left"`
	Right *datasource.Column `json:"right"`
}

// DiffRows is add, modify and delete rows struct
type DiffRows struct {
	Add    []ModifyRowValues `json:"add"`
	Modify []ModifyRowValues `json:"modify"`
	Delete []ModifyRowValues `json:"delete"`
}

// ModifyRowValues is modify row values struct between left and right
type ModifyRowValues struct {
	Left  []string `json:"left"`
	Right []string `json:"right"`
}
