package differ

import (
	"testing"

	"github.com/go-tamate/tamate/driver"
	"github.com/stretchr/testify/assert"
)

func TestDiffer_DiffColumns_IgnoreColumnNames(t *testing.T) {

	left := &driver.Schema{
		Columns: []*driver.Column{
			{Name: "id", Type: driver.ColumnTypeString},
			{Name: "name", Type: driver.ColumnTypeString},
		},
		PrimaryKey: &driver.Key{ColumnNames: []string{"id"}},
	}

	right := &driver.Schema{
		Columns: []*driver.Column{
			{Name: "id", Type: driver.ColumnTypeInt},
			{Name: "name", Type: driver.ColumnTypeString},
			{Name: "old", Type: driver.ColumnTypeString},
		},
		PrimaryKey: &driver.Key{ColumnNames: []string{"id"}},
	}

	// Ignore `old` column
	differ, err := NewDiffer(IgnoreColumn("old"))
	if assert.NoError(t, err) {
		d, err := differ.DiffColumns(left, right)
		if assert.NoError(t, err) {
			assert.Equal(t, len(d.Left), 1)
			assert.Equal(t, len(d.Right), 1)
		}
	}
}

func TestDiffer_DiffRows_IgnoreColumnNames(t *testing.T) {

	scl := &driver.Schema{
		Columns: []*driver.Column{
			{Name: "id", Type: driver.ColumnTypeString},
			{Name: "name", Type: driver.ColumnTypeString},
			{Name: "old", Type: driver.ColumnTypeString},
		},
		PrimaryKey: &driver.Key{
			KeyType:     driver.KeyTypePrimary,
			ColumnNames: []string{"id"},
		},
	}
	gbkl1 := make(driver.GroupByKey)
	gbkl1[scl.PrimaryKey.String()] = []*driver.GenericColumnValue{
		{
			Column: &driver.Column{Type: driver.ColumnTypeString},
			Value:  "id0",
		},
	}
	gbkl2 := make(driver.GroupByKey)
	gbkl2[scl.PrimaryKey.String()] = []*driver.GenericColumnValue{
		{
			Column: &driver.Column{Type: driver.ColumnTypeString},
			Value:  "id1",
		},
	}
	leftRows := []*driver.Row{
		{GroupByKey: gbkl1, Values: newRowValuesFromString(map[string]string{"id": "id0", "name": "name0", "old": "old0"})},
		{GroupByKey: gbkl2, Values: newRowValuesFromString(map[string]string{"id": "id1", "name": "name1", "old": "old1"})},
	}

	scr := &driver.Schema{
		Columns: []*driver.Column{
			{Name: "id", Type: driver.ColumnTypeString},
			{Name: "name", Type: driver.ColumnTypeString},
			{Name: "old", Type: driver.ColumnTypeString},
		},
		PrimaryKey: &driver.Key{
			KeyType:     driver.KeyTypePrimary,
			ColumnNames: []string{"id"},
		},
	}
	gbkr1 := make(driver.GroupByKey)
	gbkr1[scr.PrimaryKey.String()] = []*driver.GenericColumnValue{
		{
			Column: &driver.Column{Type: driver.ColumnTypeString},
			Value:  "id0",
		},
	}
	gbkr2 := make(driver.GroupByKey)
	gbkr2[scr.PrimaryKey.String()] = []*driver.GenericColumnValue{
		{
			Column: &driver.Column{Type: driver.ColumnTypeString},
			Value:  "id1",
		},
	}
	gbkr3 := make(driver.GroupByKey)
	gbkr3[scr.PrimaryKey.String()] = []*driver.GenericColumnValue{
		{
			Column: &driver.Column{Type: driver.ColumnTypeString},
			Value:  "id2",
		},
	}
	rightRows := []*driver.Row{
		{GroupByKey: gbkr1, Values: newRowValuesFromString(map[string]string{"id": "id0", "name": "name0_modified", "old": "old0_modified"})},
		{GroupByKey: gbkr2, Values: newRowValuesFromString(map[string]string{"id": "id1", "name": "name1", "old": "old1_modified"})},
		{GroupByKey: gbkr3, Values: newRowValuesFromString(map[string]string{"id": "id2", "name": "name2", "old": "old2"})},
	}

	// Ignore `old` column
	differ, err := NewDiffer(IgnoreColumn("old"))
	if assert.NoError(t, err) {
		diff, err := differ.DiffRows(scl, leftRows, rightRows)
		if assert.NoError(t, err) {
			assert.Equal(t, len(diff.Left), 1)
			assert.Equal(t, len(diff.Right), 2)
		}
	}
}
