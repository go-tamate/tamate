package differ

import (
	"log"

	"github.com/Mitu217/tamate/datasource"
	"github.com/Mitu217/tamate/datasource/handler"
)

// TargetTable is diff target schema struct
type TargetTable struct {
	Datasource datasource.Datasource
	SchemaName string
}

// NewTargetTable is create table instance method
func NewTargetTable(ds datasource.Datasource, scn string) (*TargetTable, error) {
	return &TargetTable{
		Datasource: ds,
		SchemaName: scn,
	}, nil
}

func (t *TargetTable) getPrimaryKeyIndex() (int, error) {
	schema, err := t.Datasource.GetSchema(t.SchemaName)
	if err != nil {
		return -1, err
	}
	return schema.GetPrimaryKeyIndex(), nil
}

// Differ is diff between tables struct
type Differ struct {
	Left  *TargetTable
	Right *TargetTable
}

// NewDiffer is create differ instance method
func NewDiffer(left *TargetTable, right *TargetTable) (*Differ, error) {
	d := &Differ{
		Left:  left,
		Right: right,
	}
	return d, nil
}

// DiffColumns is get diff columns method
func (d *Differ) diffColumns() (*DiffColumns, error) {
	// Get Schemas
	srcSchema, err := d.Left.Datasource.GetSchema(d.Left.SchemaName)
	if err != nil {
		return nil, err
	}
	dstSchema, err := d.Right.Datasource.GetSchema(d.Right.SchemaName)
	if err != nil {
		return nil, err
	}

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

func getModifyColumnValues(left *handler.Column, right *handler.Column) (*ModifyColumnValues, error) {
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
func (d *Differ) DiffRows() (*DiffRows, error) {
	// Get target rows
	err := d.Left.Datasource.Open()
	if err != nil {
		return nil, err
	}
	defer d.Left.Datasource.Close()
	srcRows, err := d.Left.Datasource.GetRows(d.Left.SchemaName)
	if err != nil {
		return nil, err
	}
	err = d.Right.Datasource.Open()
	if err != nil {
		return nil, err
	}
	defer d.Right.Datasource.Close()
	dstRows, err := d.Right.Datasource.GetRows(d.Right.SchemaName)
	if err != nil {
		return nil, err
	}
	// Get diff
	leftPrimaryKeyIndex, err := d.Left.getPrimaryKeyIndex()
	if err != nil {
		return nil, err
	}
	rightPrimaryKeyIndex, err := d.Right.getPrimaryKeyIndex()
	if err != nil {
		return nil, err
	}
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
							log.Println(modifyRowValues)
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
			Left:  *left,
			Right: *right,
		}, nil
	}
	return nil, nil
}
