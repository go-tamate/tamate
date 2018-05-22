package differ

import (
	"github.com/Mitu217/tamate/datasource"
	"testing"
)

func newRowValuesFromString(ss map[string]string) datasource.RowValues {
	res := make(datasource.RowValues, len(ss))
	for k, sv := range ss {
		res[k] = &datasource.GenericColumnValue{
			ColumnType: datasource.ColumnTypeString,
			Value:      sv,
		}
	}
	return res
}

func TestDiffer_DiffRows(t *testing.T) {
	sc := &datasource.Schema{
		Columns: []*datasource.Column{
			{Name: "id", Type: datasource.ColumnTypeString},
			{Name: "name", Type: datasource.ColumnTypeString},
		},
		PrimaryKey: &datasource.PrimaryKey{ColumnNames: []string{"id"}},
	}

	leftRows := []*datasource.Row{
		{newRowValuesFromString(map[string]string{"id": "id0", "name": "name0"})},
		{newRowValuesFromString(map[string]string{"id": "id1", "name": "name1"})},
	}
	rightRows := []*datasource.Row{
		{newRowValuesFromString(map[string]string{"id": "id0", "name": "name0_modified"})},
		{newRowValuesFromString(map[string]string{"id": "id1", "name": "name1"})},
		{newRowValuesFromString(map[string]string{"id": "id2", "name": "name2"})},
		{newRowValuesFromString(map[string]string{"id": "id3", "name": "name3"})},
	}

	differ, err := NewDiffer()
	if err != nil {
		t.Fatal(err)
	}

	// the same (no diff)
	/*
		{
			diff, err := differ.DiffRows(sc.PrimaryKey, leftRows, leftRows)
			if err != nil {
				t.Fatal(err)
			}

			if len(diff.Left) != 0 || len(diff.Right) != 0 {
				t.Fatalf("expect: no row in diff, actual: diff.Left: %d, diff.Right: %d", len(diff.Left), len(diff.Right))
			}
		}
	*/

	{
		diff, err := differ.DiffRows(sc.PrimaryKey, leftRows, rightRows)
		if err != nil {
			t.Fatal(err)
		}

		if len(diff.Left) != 1 {
			t.Fatalf("expect: 1 row in diff.Left, actual: %d", len(diff.Left))
		}
		if len(diff.Right) != 3 {
			t.Fatalf("expect: 3 rows in diff.Right, actual: %d", len(diff.Right))
		}
	}

}

func TestDiffer_DiffColumns(t *testing.T) {
	differ, err := NewDiffer()
	if err != nil {
		t.Fatal(err)
	}

	{
		left := &datasource.Schema{
			Columns: []*datasource.Column{
				{Name: "id", Type: datasource.ColumnTypeString},
				{Name: "name", Type: datasource.ColumnTypeString},
			},
			PrimaryKey: &datasource.PrimaryKey{ColumnNames: []string{"id"}},
		}

		right := &datasource.Schema{
			Columns: []*datasource.Column{
				{Name: "id", Type: datasource.ColumnTypeInt},
				{Name: "name", Type: datasource.ColumnTypeString},
			},
			PrimaryKey: &datasource.PrimaryKey{ColumnNames: []string{"id"}},
		}

		d, err := differ.DiffColumns(left, right)
		if err != nil {
			t.Fatal(err)
		}

		if len(d.Left) != 1 || len(d.Right) != 1 {
			t.Fatalf("expect: 1 columns modified, actual: left: %d, right: %d", len(d.Left), len(d.Right))
		}
	}
}
