package differ

import (
	"errors"
	"github.com/Mitu217/tamate/datasource"
	"github.com/Mitu217/tamate/datasource/handler"
)

// TargetSchema is diff target schema struct
type TargetSchema struct {
	Datasource datasource.Datasource
	SchemaName string
}

// Differ :
type Differ struct {
	Left  TargetSchema
	Right TargetSchema
}

// NewSchemaDiffer :
func NewSchemaDiffer(left TargetSchema, right TargetSchema) (*Differ, error) {
	return nil, errors.New("not support NewSchemaDiffer()")
}

// NewRowsDiffer :
func NewRowsDiffer(left TargetSchema, right TargetSchema) (*Differ, error) {
	d := &Differ{
		Left:  left,
		Right: right,
	}
	diffColumns, err := d.diffColumns()
	if err != nil {
		return nil, err
	}
	if diffColumns.IsDiff() {
		return nil, errors.New("schema between two data does not match")
	}
	return d, err
}

// DiffColumns :
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
	for i := 0; i < 2; i++ {
		for _, srcColumn := range srcSchema.Columns {
			found := false
			for _, dstColumn := range dstSchema.Columns {
				if srcColumn.Name == dstColumn.Name {
					found = true

					// Modify
					if i == 0 {
						modifyColumn := handler.Column{
							Name: srcColumn.Name,
						}
						modify := false
						if srcColumn.Type != dstColumn.Type {
							modify = true
							modifyColumn.Type = dstColumn.Type
						}
						if srcColumn.NotNull != dstColumn.NotNull {
							modify = true
							modifyColumn.NotNull = dstColumn.NotNull
						}
						if srcColumn.AutoIncrement != dstColumn.AutoIncrement {
							modify = true
							modifyColumn.AutoIncrement = dstColumn.AutoIncrement
						}

						if modify {
							diff.Modify = append(diff.Modify, modifyColumn)
						}
					}

					break
				}
			}
			if !found {
				if i == 0 {
					// Add
					diff.Add = append(diff.Add, srcColumn)
				} else {
					// Delete
					diff.Delete = append(diff.Delete, srcColumn)
				}
			}
		}

		// Swap
		if i == 0 {
			srcSchema, dstSchema = dstSchema, srcSchema
		}
	}

	return diff, nil
}

// DiffRows :
func (d *Differ) DiffRows() (*DiffRows, error) {
	// Get Rows
	srcRows, err := d.Left.Datasource.GetRows(d.Left.SchemaName)
	if err != nil {
		return nil, err
	}
	dstRows, err := d.Right.Datasource.GetRows(d.Right.SchemaName)
	if err != nil {
		return nil, err
	}

	diff := &DiffRows{
		Add:    &handler.Rows{},
		Delete: &handler.Rows{},
		Modify: &handler.Rows{},
	}

	// FIXME
	pki := 0 //d.Schema.ColumnIndex(d.Schema.PrimaryKey)
	if pki == -1 {
		return nil, errors.New("Primary key not found in `" + d.Left.SchemaName + "`")
	}

	// Get diff
	schema, err := d.Left.Datasource.GetSchema(d.Left.SchemaName)
	if err != nil {
		return nil, err
	}
	columnNames := schema.ColumnNames()
	for i := 0; i < 2; i++ {
		for _, srcValue := range srcRows.Values {
			srcPrimaryValue := srcValue[pki]
			found := false
			for _, dstValue := range dstRows.Values {
				dstPrimaryValue := dstValue[pki]
				if srcPrimaryValue == dstPrimaryValue {
					found = true

					// Modify
					if i == 0 {
						modifyValues := make([]string, len(columnNames))
						modify := false
						for ci := range columnNames {
							if srcValue[ci] != dstValue[ci] {
								// Modify column
								modifyValues[ci] = dstValue[ci]
								modify = true
								break
							}
						}
						if modify {
							modifyValues[pki] = srcPrimaryValue
							diff.Modify.Values = append(diff.Modify.Values, [][]string{modifyValues}...)
						}
					}
					break
				}
			}
			if !found {
				if i == 0 {
					// Add
					diff.Add.Values = append(diff.Add.Values, [][]string{srcValue}...)
				} else {
					// Delete
					diff.Delete.Values = append(diff.Delete.Values, [][]string{srcValue}...)
				}
			}
		}

		// Swap
		if i == 0 {
			srcRows, dstRows = dstRows, srcRows
		}
	}
	return diff, nil
}
