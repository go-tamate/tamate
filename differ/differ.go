package differ

import (
	"errors"

	"github.com/Mitu217/tamate/table"
	"github.com/Mitu217/tamate/table/schema"
)

// Differ :
type Differ struct {
	Schema      *schema.Schema
	LeftSource  table.Table
	RightSource table.Table
}

// NewSchemaDiffer :
func NewSchemaDiffer(sc *schema.Schema, leftSrc table.Table, rightSrc table.Table) (*Differ, error) {
	d := &Differ{
		Schema:      sc,
		LeftSource:  leftSrc,
		RightSource: rightSrc,
	}
	return d, nil
}

// NewRowsDiffer :
func NewRowsDiffer(leftSrc table.Table, rightSrc table.Table) (*Differ, error) {
	d := &Differ{
		LeftSource:  leftSrc,
		RightSource: rightSrc,
	}

	diffColumns, err := d.diffColumns()
	if err != nil {
		return nil, err
	}
	if diffColumns.IsDiff() {
		return nil, errors.New("schema between two data does not match")
	}

	sc, err := leftSrc.GetSchema()
	if err != nil {
		return nil, err
	}
	d.Schema = sc
	return d, err
}

// DiffColumns :
func (d *Differ) diffColumns() (*DiffColumns, error) {
	// Get Schemas
	srcSchemas, err := d.LeftSource.GetSchema()
	if err != nil {
		return nil, err
	}
	dstSchemas, err := d.RightSource.GetSchema()
	if err != nil {
		return nil, err
	}

	// Get diff
	diff := &DiffColumns{}
	for i := 0; i < 2; i++ {
		for _, srcColumn := range srcSchemas.Columns {
			found := false
			for _, dstColumn := range dstSchemas.Columns {
				if srcColumn.Name == dstColumn.Name {
					found = true

					// Modify
					if i == 0 {
						modifyColumn := schema.Column{
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
			srcSchemas, dstSchemas = dstSchemas, srcSchemas
		}
	}

	return diff, nil
}

// DiffRows :
func (d *Differ) DiffRows() (*DiffRows, error) {
	// Get Rows
	srcRows, err := d.LeftSource.GetRows()
	if err != nil {
		return nil, err
	}
	dstRows, err := d.RightSource.GetRows()
	if err != nil {
		return nil, err
	}

	pki := d.Schema.ColumnIndex(d.Schema.PrimaryKey)
	if pki == -1 {
		return nil, errors.New("Primary key not found in `" + d.Schema.Name + "`")
	}

	// Get diff
	columnNames := d.Schema.ColumnNames()
	diff := &DiffRows{
		Add:    &table.Rows{},
		Delete: &table.Rows{},
		Modify: &table.Rows{},
	}
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
						for ci, _ := range columnNames {
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
