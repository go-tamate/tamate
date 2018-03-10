package differ

import (
	"errors"

	"github.com/Mitu217/tamate/datasource"
	"github.com/Mitu217/tamate/schema"
)

// Diff :
type Diff struct {
	Add    datasource.Rows
	Modify datasource.Rows
	Delete datasource.Rows
}

// Differ :
type Differ struct {
	LeftSource  datasource.DataSource
	RightSource datasource.DataSource
}

// NewDiffer :
func NewDiffer(leftSrc datasource.DataSource, rightSrc datasource.DataSource) *Differ {
	return &Differ{
		LeftSource:  leftSrc,
		RightSource: rightSrc,
	}
}

// DiffRows :
func (d *Differ) DiffRows(sc schema.Schema) (*Diff, error) {
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
	// TODO: PrimaryKey時代はDataStoreからも引っ張れるがどうするか
	primaryKey := sc.GetPrimaryKey()
	srcPrimaryIndex := contains(srcRows.Columns, primaryKey)
	if srcPrimaryIndex == -1 {
		return nil, errors.New("TODO")
	}
	dstPrimaryIndex := contains(dstRows.Columns, primaryKey)
	if dstPrimaryIndex == -1 {
		return nil, errors.New("TODO")
	}

	// Get diff
	diff := &Diff{}
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
						// スキーマ基準で差分を比較する
						modifyValues := make([]string, len(srcValue))
						modify := false
						for _, column := range sc.GetColumns() {
							// TODO index == -1 チェック
							srcColumnIndex := contains(srcRows.Columns, column.Name)
							srcColumnValue := srcValue[srcColumnIndex]
							dstColumnIndex := contains(dstRows.Columns, column.Name)
							dstColumnValue := dstValue[dstColumnIndex]
							// Primaryは必須
							if srcPrimaryIndex == srcColumnIndex {
								modifyValues[srcColumnIndex] = dstColumnValue
							}
							if srcColumnValue != dstColumnValue {
								modifyValues[srcColumnIndex] = dstColumnValue
								modify = true
							}
						}
						if modify {
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
					//TODO schemaが異なるときに不具合がおきるはずなので修正必須
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
