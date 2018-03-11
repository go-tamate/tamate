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
		for _, srcColumn := range srcSchemas.GetColumns() {
			found := false
			for _, dstColumn := range dstSchemas.GetColumns() {
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
	columnNames := make([]string, len(d.Schema.GetColumns()))
	for i, column := range d.Schema.GetColumns() {
		columnNames[i] = column.Name
	}
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
						modifyValues := make([]string, len(columnNames))
						modify := false
						for _, columnName := range columnNames {
							srcColumnIndex := contains(srcRows.Columns, columnName)
							srcColumnValue := srcValue[srcColumnIndex]
							dstColumnIndex := contains(dstRows.Columns, columnName)
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
