package tamate

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDiffer_DiffColumns_IgnoreColumnNames(t *testing.T) {

	left := &Schema{
		Columns: []*Column{
			{Name: "id", Type: ColumnTypeString},
			{Name: "name", Type: ColumnTypeString},
		},
		PrimaryKey: &Key{ColumnNames: []string{"id"}},
	}

	right := &Schema{
		Columns: []*Column{
			{Name: "id", Type: ColumnTypeInt},
			{Name: "name", Type: ColumnTypeString},
			{Name: "old", Type: ColumnTypeString},
		},
		PrimaryKey: &Key{ColumnNames: []string{"id"}},
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

	scl := &Schema{
		Columns: []*Column{
			{Name: "id", Type: ColumnTypeString},
			{Name: "name", Type: ColumnTypeString},
			{Name: "old", Type: ColumnTypeString},
		},
		PrimaryKey: &Key{
			KeyType:     KeyTypePrimary,
			ColumnNames: []string{"id"},
		},
	}
	gbkl1 := make(GroupByKey)
	gbkl1[scl.PrimaryKey.String()] = []*GenericColumnValue{
		{
			Column: &Column{Type: ColumnTypeString},
			Value:  "id0",
		},
	}
	gbkl2 := make(GroupByKey)
	gbkl2[scl.PrimaryKey.String()] = []*GenericColumnValue{
		{
			Column: &Column{Type: ColumnTypeString},
			Value:  "id1",
		},
	}
	leftRows := []*Row{
		{GroupByKey: gbkl1, Values: newRowValuesFromString(map[string]string{"id": "id0", "name": "name0", "old": "old0"})},
		{GroupByKey: gbkl2, Values: newRowValuesFromString(map[string]string{"id": "id1", "name": "name1", "old": "old1"})},
	}

	scr := &Schema{
		Columns: []*Column{
			{Name: "id", Type: ColumnTypeString},
			{Name: "name", Type: ColumnTypeString},
			{Name: "old", Type: ColumnTypeString},
		},
		PrimaryKey: &Key{
			KeyType:     KeyTypePrimary,
			ColumnNames: []string{"id"},
		},
	}
	gbkr1 := make(GroupByKey)
	gbkr1[scr.PrimaryKey.String()] = []*GenericColumnValue{
		{
			Column: &Column{Type: ColumnTypeString},
			Value:  "id0",
		},
	}
	gbkr2 := make(GroupByKey)
	gbkr2[scr.PrimaryKey.String()] = []*GenericColumnValue{
		{
			Column: &Column{Type: ColumnTypeString},
			Value:  "id1",
		},
	}
	gbkr3 := make(GroupByKey)
	gbkr3[scr.PrimaryKey.String()] = []*GenericColumnValue{
		{
			Column: &Column{Type: ColumnTypeString},
			Value:  "id2",
		},
	}
	rightRows := []*Row{
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
