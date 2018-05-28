package differ

import (
	"testing"

	"context"

	"github.com/Mitu217/tamate/datasource"

	"github.com/araddon/dateparse"
)

func newRowValuesFromString(ss map[string]string) datasource.RowValues {
	res := make(datasource.RowValues, len(ss))
	for k, sv := range ss {
		res[k] = &datasource.GenericColumnValue{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  sv,
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
		PrimaryKey: &datasource.Key{
			KeyType:     datasource.KeyTypePrimary,
			ColumnNames: []string{"id"},
		},
	}

	gbkl1 := make(map[*datasource.Key][]*datasource.GenericColumnValue)
	gbkl1[sc.PrimaryKey] = []*datasource.GenericColumnValue{
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "id0",
		},
	}
	gbkl2 := make(map[*datasource.Key][]*datasource.GenericColumnValue)
	gbkl2[sc.PrimaryKey] = []*datasource.GenericColumnValue{
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "id1",
		},
	}
	leftRows := []*datasource.Row{
		{GroupByKey: gbkl1, Values: newRowValuesFromString(map[string]string{"id": "id0", "name": "name0"})},
		{GroupByKey: gbkl2, Values: newRowValuesFromString(map[string]string{"id": "id1", "name": "name1"})},
	}

	gbkr1 := make(map[*datasource.Key][]*datasource.GenericColumnValue)
	gbkr1[sc.PrimaryKey] = []*datasource.GenericColumnValue{
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "id0",
		},
	}
	gbkr2 := make(map[*datasource.Key][]*datasource.GenericColumnValue)
	gbkr2[sc.PrimaryKey] = []*datasource.GenericColumnValue{
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "id1",
		},
	}
	gbkr3 := make(map[*datasource.Key][]*datasource.GenericColumnValue)
	gbkr3[sc.PrimaryKey] = []*datasource.GenericColumnValue{
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "id2",
		},
	}
	gbkr4 := make(map[*datasource.Key][]*datasource.GenericColumnValue)
	gbkr4[sc.PrimaryKey] = []*datasource.GenericColumnValue{
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "id3",
		},
	}
	rightRows := []*datasource.Row{
		{GroupByKey: gbkr1, Values: newRowValuesFromString(map[string]string{"id": "id0", "name": "name0_modified"})},
		{GroupByKey: gbkr2, Values: newRowValuesFromString(map[string]string{"id": "id1", "name": "name1"})},
		{GroupByKey: gbkr3, Values: newRowValuesFromString(map[string]string{"id": "id2", "name": "name2"})},
		{GroupByKey: gbkr4, Values: newRowValuesFromString(map[string]string{"id": "id3", "name": "name3"})},
	}

	differ, err := NewDiffer()
	if err != nil {
		t.Fatal(err)
	}

	// the same (no diff)
	{
		diff, err := differ.DiffRows(sc, leftRows, leftRows)
		if err != nil {
			t.Fatal(err)
		}

		if len(diff.Left) != 0 || len(diff.Right) != 0 {
			t.Fatalf("expect: no row in diff, actual: diff.Left: %d, diff.Right: %d", len(diff.Left), len(diff.Right))
		}
	}

	{
		diff, err := differ.DiffRows(sc, leftRows, rightRows)
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

func TestDiffer_DiffRows_CompositeKey(t *testing.T) {
	sc := &datasource.Schema{
		Columns: []*datasource.Column{
			{Name: "id", Type: datasource.ColumnTypeString},
			{Name: "name", Type: datasource.ColumnTypeString},
			{Name: "comment", Type: datasource.ColumnTypeString},
		},
		PrimaryKey: &datasource.Key{
			KeyType:     datasource.KeyTypePrimary,
			ColumnNames: []string{"id", "name"},
		},
	}

	gbkl_1 := make(map[*datasource.Key][]*datasource.GenericColumnValue)
	gbkl_1[sc.PrimaryKey] = []*datasource.GenericColumnValue{
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "id0",
		},
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "name0",
		},
	}
	gbkl_2 := make(map[*datasource.Key][]*datasource.GenericColumnValue)
	gbkl_2[sc.PrimaryKey] = []*datasource.GenericColumnValue{
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "id1",
		},
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "name1",
		},
	}
	leftRows := []*datasource.Row{
		{GroupByKey: gbkl_1, Values: newRowValuesFromString(map[string]string{"id": "id0", "name": "name0", "comment": "hello, world!"})},
		{GroupByKey: gbkl_2, Values: newRowValuesFromString(map[string]string{"id": "id1", "name": "name1", "comment": "hello, world!!"})},
	}

	gbkr_1 := make(map[*datasource.Key][]*datasource.GenericColumnValue)
	gbkr_1[sc.PrimaryKey] = []*datasource.GenericColumnValue{
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "id0",
		},
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "name0",
		},
	}
	gbkr_2 := make(map[*datasource.Key][]*datasource.GenericColumnValue)
	gbkr_2[sc.PrimaryKey] = []*datasource.GenericColumnValue{
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "id1",
		},
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "name1",
		},
	}
	gbkr_3 := make(map[*datasource.Key][]*datasource.GenericColumnValue)
	gbkr_3[sc.PrimaryKey] = []*datasource.GenericColumnValue{
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "id2",
		},
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "name2",
		},
	}
	gbkr_4 := make(map[*datasource.Key][]*datasource.GenericColumnValue)
	gbkr_4[sc.PrimaryKey] = []*datasource.GenericColumnValue{
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "id3",
		},
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "name3",
		},
	}
	rightRows := []*datasource.Row{
		{GroupByKey: gbkr_1, Values: newRowValuesFromString(map[string]string{"id": "id0", "name": "name0", "comment": "hello, world!"})},
		{GroupByKey: gbkr_2, Values: newRowValuesFromString(map[string]string{"id": "id1", "name": "name1", "comment": "hello, world."})},
		{GroupByKey: gbkr_3, Values: newRowValuesFromString(map[string]string{"id": "id2", "name": "name2", "comment": "hello, world!!!"})},
		{GroupByKey: gbkr_4, Values: newRowValuesFromString(map[string]string{"id": "id3", "name": "name3", "comment": "hello, world!!!!"})},
	}

	differ, err := NewDiffer()
	if err != nil {
		t.Fatal(err)
	}

	// the same (no diff)
	{
		diff, err := differ.DiffRows(sc, leftRows, leftRows)
		if err != nil {
			t.Fatal(err)
		}

		if len(diff.Left) != 0 || len(diff.Right) != 0 {
			t.Fatalf("expect: no row in diff, actual: diff.Left: %d, diff.Right: %d", len(diff.Left), len(diff.Right))
		}
	}

	{
		diff, err := differ.DiffRows(sc, leftRows, rightRows)
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
			PrimaryKey: &datasource.Key{ColumnNames: []string{"id"}},
		}

		right := &datasource.Schema{
			Columns: []*datasource.Column{
				{Name: "id", Type: datasource.ColumnTypeInt},
				{Name: "name", Type: datasource.ColumnTypeString},
			},
			PrimaryKey: &datasource.Key{ColumnNames: []string{"id"}},
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

func TestDiffer_DiffDatetimeFormatStringColumn(t *testing.T) {
	ds, err := datasource.NewMockDatasource()
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	sc, err := ds.GetSchema(ctx, "")
	if err != nil {
		t.Fatal(err)
	}

	leftRows, err := ds.GetRows(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}

	// Change column type string -> datetime
	for i, col := range sc.Columns {
		if col.Name == "birthday" {
			sc.Columns[i].Type = datasource.ColumnTypeDatetime
		}
	}

	rightRows, err := ds.GetRows(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	for i, rrow := range rightRows {
		tv, err := dateparse.ParseAny(rrow.Values["birthday"].StringValue())
		if err != nil {
			t.Fatal(err)
		}
		rightRows[i].Values["birthday"].Value = tv
	}

	differ, err := NewDiffer()
	if err != nil {
		t.Fatal(err)
	}

	diff, err := differ.DiffRows(sc, leftRows, rightRows)
	if err != nil {
		t.Fatal(err)
	}

	if len(diff.Left) != 0 {
		t.Fatalf("len(diff.Left) must be 0, but actual: %+v", len(diff.Left))
	}
	if len(diff.Right) != 0 {
		t.Fatalf("len(diff.Right) must be 0, but actual: %+v", len(diff.Right))
	}
}
