package differ

import (
	"testing"

	"github.com/Mitu217/tamate/datasource"
	"github.com/stretchr/testify/assert"
)

func TestDiffer_DiffColumns_IgnoreColumnNames(t *testing.T) {

	left := &datasource.Schema{
		Columns: []*datasource.Column{
			{Name: "id", Type: datasource.ColumnTypeString},
			{Name: "name", Type: datasource.ColumnTypeString},
		},
		PrimaryKey: &datasource.Key{ColumnNames: []string{"id"}},
	}

	right := &datasource.Schema{
		Columns: []*datasource.Column{
			{Name: "id", Type: datasource.ColumnTypeInt},
			{Name: "name", Type: datasource.ColumnTypeString},
			{Name: "old", Type: datasource.ColumnTypeString},
		},
		PrimaryKey: &datasource.Key{ColumnNames: []string{"id"}},
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

	scl := &datasource.Schema{
		Columns: []*datasource.Column{
			{Name: "id", Type: datasource.ColumnTypeString},
			{Name: "name", Type: datasource.ColumnTypeString},
			{Name: "old", Type: datasource.ColumnTypeString},
		},
		PrimaryKey: &datasource.Key{
			KeyType:     datasource.KeyTypePrimary,
			ColumnNames: []string{"id"},
		},
	}
	gbkl1 := make(datasource.GroupByKey)
	gbkl1[scl.PrimaryKey.String()] = []*datasource.GenericColumnValue{
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "id0",
		},
	}
	gbkl2 := make(datasource.GroupByKey)
	gbkl2[scl.PrimaryKey.String()] = []*datasource.GenericColumnValue{
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "id1",
		},
	}
	leftRows := []*datasource.Row{
		{GroupByKey: gbkl1, Values: newRowValuesFromString(map[string]string{"id": "id0", "name": "name0", "old": "old0"})},
		{GroupByKey: gbkl2, Values: newRowValuesFromString(map[string]string{"id": "id1", "name": "name1", "old": "old1"})},
	}

	scr := &datasource.Schema{
		Columns: []*datasource.Column{
			{Name: "id", Type: datasource.ColumnTypeString},
			{Name: "name", Type: datasource.ColumnTypeString},
		},
		PrimaryKey: &datasource.Key{
			KeyType:     datasource.KeyTypePrimary,
			ColumnNames: []string{"id"},
		},
	}
	gbkr1 := make(datasource.GroupByKey)
	gbkr1[scr.PrimaryKey.String()] = []*datasource.GenericColumnValue{
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "id0",
		},
	}
	gbkr2 := make(datasource.GroupByKey)
	gbkr2[scr.PrimaryKey.String()] = []*datasource.GenericColumnValue{
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "id1",
		},
	}
	gbkr3 := make(datasource.GroupByKey)
	gbkr3[scr.PrimaryKey.String()] = []*datasource.GenericColumnValue{
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "id2",
		},
	}
	rightRows := []*datasource.Row{
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
