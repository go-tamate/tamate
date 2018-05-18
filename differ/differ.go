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

// Differ is diff between tables struct
type Differ struct {
}

// NewDiffer is create differ instance method
func NewDiffer() (*Differ, error) {
	d := &Differ{}
	return d, nil
}

// DiffColumns is get diff columns method
func (d *Differ) DiffColumns(left, right *datasource.Schema) (*DiffColumns, error) {
	// Get Schemas
	srcSchema := left
	dstSchema := right

	// Get diff
	diff := &DiffColumns{}
	for _, pattern := range []string{"Normal", "Reverse"} {
		for _, srcColumn := range srcSchema.Columns {
			found := false
			for _, dstColumn := range dstSchema.Columns {
				if srcColumn.Name == dstColumn.Name {
					found = true
					if pattern == "Normal" {
						// Modify
						modifyColumnValues, err := getModifyColumnValues(srcColumn, dstColumn)
						if err != nil {
							return nil, err
						}
						if modifyColumnValues != nil {
							diff.Modify = append(diff.Modify, *modifyColumnValues)
						}
					}
					break
				}
			}
			if !found {
				if pattern == "Normal" {
					// Add
					modifyColumnValues, err := getModifyColumnValues(nil, srcColumn)
					if err != nil {
						return nil, err
					}
					diff.Add = append(diff.Add, *modifyColumnValues)
				} else {
					// Delete
					modifyColumnValues, err := getModifyColumnValues(srcColumn, nil)
					if err != nil {
						return nil, err
					}
					diff.Delete = append(diff.Delete, *modifyColumnValues)
				}
			}
		}
		// swap
		srcSchema, dstSchema = dstSchema, srcSchema
	}

	return diff, nil
}

func getModifyColumnValues(left, right *datasource.Column) (*ModifyColumnValues, error) {
	modify := false
	if left == nil || right == nil {
		modify = true
	}
	if left.Type != right.Type {
		modify = true
	}
	if left.NotNull != right.NotNull {
		modify = true
	}
	if left.AutoIncrement != right.AutoIncrement {
		modify = true
	}
	if modify {
		return &ModifyColumnValues{
			Left:  left,
			Right: right,
		}, nil
	}
	return nil, nil
}

// DiffRows is get diff rows method
func (d *Differ) DiffRows(sc *datasource.Schema, left, right *datasource.Rows) (*DiffRows, error) {
	srcRows := right
	dstRows := left
	leftPrimaryKeyIndex := sc.GetPrimaryKeyIndex()
	rightPrimaryKeyIndex := sc.GetPrimaryKeyIndex()

	diff := &DiffRows{}
	for _, pattern := range []string{"Normal", "Reverse"} {
		for i, srcValue := range srcRows.Values {
			found := false
			if leftPrimaryKeyIndex != -1 {
				// diff by primary key
				for _, dstValue := range dstRows.Values {
					if srcValue[leftPrimaryKeyIndex] == dstValue[rightPrimaryKeyIndex] {
						found = true
						if pattern == "Normal" {
							modifyRowValues, err := getModifyRowValues(&srcValue, &dstValue)
							if err != nil {
								return nil, err
							}
							if modifyRowValues != nil {
								diff.Modify = append(diff.Modify, *modifyRowValues)
							}
						}
						break
					}
				}
			} else {
				// simple diff when not setting primary key
				if i < len(dstRows.Values) {
					dstValue := dstRows.Values[i]
					found = true
					if pattern == "Normal" {
						modifyRowValues, err := getModifyRowValues(&srcValue, &dstValue)
						if err != nil {
							return nil, err
						}
						if modifyRowValues != nil {
							diff.Modify = append(diff.Modify, *modifyRowValues)
						}
					}
				}
			}
			if !found {
				if pattern == "Normal" {
					// Add
					modifyRowValues, err := getModifyRowValues(nil, &srcValue)
					if err != nil {
						return nil, err
					}
					diff.Add = append(diff.Add, *modifyRowValues)
				} else {
					// Delete
					modifyRowValues, err := getModifyRowValues(&srcValue, nil)
					if err != nil {
						return nil, err
					}
					diff.Delete = append(diff.Delete, *modifyRowValues)
				}
			}
		}
		// swap
		srcRows, dstRows = dstRows, srcRows
	}
	return diff, nil
}

func getModifyRowValues(left *[]string, right *[]string) (*ModifyRowValues, error) {
	modify := false
	if left == nil && right == nil {
		modify = false
	} else if left == nil || right == nil {
		modify = true
	} else {
		if len(*left) == len(*right) {
			for i := range *left {
				if (*left)[i] != (*right)[i] {
					modify = true
				}
			}
		} else {
			modify = true
		}
	}
	if modify {
		if left == nil {
			left = &[]string{}
		}
		if right == nil {
			right = &[]string{}
		}
		return &ModifyRowValues{
			Left:  *right,
			Right: *left,
		}, nil
	}
	return nil, nil
}
