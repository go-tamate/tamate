package differ

import (
	"github.com/Mitu217/tamate/datasource/handler"
)

// DiffColumns is add, modify and delete columns struct
type DiffColumns struct {
	Add    []ModifyColumnValues
	Modify []ModifyColumnValues
	Delete []ModifyColumnValues
}

// ModifyColumnValues is modify column values struct between left and right
type ModifyColumnValues struct {
	Left  *handler.Column
	Right *handler.Column
}

// DiffRows is add, modify and delete rows struct
type DiffRows struct {
	Add    []ModifyRowValues
	Modify []ModifyRowValues
	Delete []ModifyRowValues
}

// ModifyRowValues is modify row values struct between left and right
type ModifyRowValues struct {
	Left  *[]string
	Right *[]string
}
