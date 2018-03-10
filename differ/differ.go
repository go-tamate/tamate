package differ

import (
	"errors"

	"github.com/Mitu217/tamate/datasource"
	"github.com/Mitu217/tamate/schema"
)

// Differ :
type Differ struct {
	Schema      schema.Schema
	LeftSource  datasource.DataSource
	RightSource datasource.DataSource
}

// NewSchemaDiffer :
func NewSchemaDiffer(sc schema.Schema, leftSrc datasource.DataSource, rightSrc datasource.DataSource) (*Differ, error) {
	d := &Differ{
		Schema:      sc,
		LeftSource:  leftSrc,
		RightSource: rightSrc,
	}
	return d, nil
}

// NewRowsDiffer :
func NewRowsDiffer(leftSrc datasource.DataSource, rightSrc datasource.DataSource) (*Differ, error) {
	d := &Differ{
		LeftSource:  leftSrc,
		RightSource: rightSrc,
	}

	diffColumns, err := d.diffColumns()
	if err != nil {
		return nil, err
	}
	if !diffColumns.IsDiff() {
		return nil, errors.New("Schema between two data does not match")
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
	// Get Rows
	srcRows, err := d.LeftSource.GetRows()
	if err != nil {
		return nil, err
	}
	dstRows, err := d.RightSource.GetRows()
	if err != nil {
		return nil, err
	}

	// Get diff
	diff := &DiffColumns{}
	for i := 0; i < 2; i++ {
		for _, srcColumn := range srcRows.Columns {
			found := false
			for _, dstColumn := range dstRows.Columns {
				if srcColumn == dstColumn {
					found = true
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
			srcRows, dstRows = dstRows, srcRows
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

	// Get Primary
	primaryKey := d.Schema.GetPrimaryKey()
	srcPrimaryIndex := contains(srcRows.Columns, primaryKey)
	if srcPrimaryIndex == -1 {
		return nil, errors.New("Not defineded PrimaryKey in `" + d.Schema.GetTableName() + "` Schema")
	}
	dstPrimaryIndex := contains(dstRows.Columns, primaryKey)
	if dstPrimaryIndex == -1 {
		return nil, errors.New("Not defineded PrimaryKey in `" + d.Schema.GetTableName() + "` Schema")
	}

	// Get diff
	columnNames := d.Schema.GetColumnNames()
	diff := &DiffRows{
		Add: &datasource.Rows{
			Columns: columnNames,
		},
		Delete: &datasource.Rows{
			Columns: columnNames,
		},
		Modify: &datasource.Rows{
			Columns: columnNames,
		},
	}
	for i := 0; i < 2; i++ {
		for _, srcValue := range srcRows.Values {
			srcPrimaryValue := srcValue[srcPrimaryIndex]
			found := false
			for _, dstValue := range dstRows.Values {
				dstPrimaryValue := dstValue[dstPrimaryIndex]
				if srcPrimaryValue == dstPrimaryValue {
					found = true

					// Modify
					if i == 0 {
						modifyValues := make([]string, len(d.Schema.GetColumns()))
						modify := false
						for _, column := range d.Schema.GetColumns() {
							srcColumnIndex := contains(srcRows.Columns, column.Name)
							srcColumnValue := srcValue[srcColumnIndex]
							dstColumnIndex := contains(dstRows.Columns, column.Name)
							dstColumnValue := dstValue[dstColumnIndex]
							if srcPrimaryIndex == srcColumnIndex {
								// Skip Primarykey column
								continue
							}
							if dstColumnIndex == -1 {
								// Delete column
								modifyValues[srcColumnIndex] = ""
								modify = true
								break
							}
							if srcColumnValue != dstColumnValue {
								// Modify column
								modifyValues[srcColumnIndex] = dstColumnValue
								modify = true
								break
							}
						}
						if modify {
							modifyValues[srcPrimaryIndex] = srcPrimaryValue
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

func contains(s []string, e string) int {
	for i, v := range s {
		if e == v {
			return i
		}
	}
	return -1
}
