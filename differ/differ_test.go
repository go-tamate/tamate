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

	scl := &datasource.Schema{
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
	gbkl1[scl.PrimaryKey] = []*datasource.GenericColumnValue{
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "id0",
		},
	}
	gbkl2 := make(map[*datasource.Key][]*datasource.GenericColumnValue)
	gbkl2[scl.PrimaryKey] = []*datasource.GenericColumnValue{
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "id1",
		},
	}
	leftRows := []*datasource.Row{
		{GroupByKey: gbkl1, Values: newRowValuesFromString(map[string]string{"id": "id0", "name": "name0"})},
		{GroupByKey: gbkl2, Values: newRowValuesFromString(map[string]string{"id": "id1", "name": "name1"})},
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
	gbkr1 := make(map[*datasource.Key][]*datasource.GenericColumnValue)
	gbkr1[scr.PrimaryKey] = []*datasource.GenericColumnValue{
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "id0",
		},
	}
	gbkr2 := make(map[*datasource.Key][]*datasource.GenericColumnValue)
	gbkr2[scr.PrimaryKey] = []*datasource.GenericColumnValue{
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "id1",
		},
	}
	gbkr3 := make(map[*datasource.Key][]*datasource.GenericColumnValue)
	gbkr3[scr.PrimaryKey] = []*datasource.GenericColumnValue{
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "id2",
		},
	}
	gbkr4 := make(map[*datasource.Key][]*datasource.GenericColumnValue)
	gbkr4[scr.PrimaryKey] = []*datasource.GenericColumnValue{
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
		diff, err := differ.DiffRows(scl, scl, leftRows, leftRows)
		if err != nil {
			t.Fatal(err)
		}

		if len(diff.Left) != 0 || len(diff.Right) != 0 {
			t.Fatalf("expect: no row in diff, actual: diff.Left: %d, diff.Right: %d", len(diff.Left), len(diff.Right))
		}
	}

	// wrong schema is chosen
	{
		_, err := differ.DiffRows(scl, scr, leftRows, leftRows)
		if err == nil {
			t.Fatal(err)
		}
	}

	{
		diff, err := differ.DiffRows(scl, scr, leftRows, rightRows)
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

	scl := &datasource.Schema{
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
	gbkl1 := make(map[*datasource.Key][]*datasource.GenericColumnValue)
	gbkl1[scl.PrimaryKey] = []*datasource.GenericColumnValue{
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "id0",
		},
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "name0",
		},
	}
	gbkl2 := make(map[*datasource.Key][]*datasource.GenericColumnValue)
	gbkl2[scl.PrimaryKey] = []*datasource.GenericColumnValue{
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
		{GroupByKey: gbkl1, Values: newRowValuesFromString(map[string]string{"id": "id0", "name": "name0", "comment": "hello, world!"})},
		{GroupByKey: gbkl2, Values: newRowValuesFromString(map[string]string{"id": "id1", "name": "name1", "comment": "hello, world!!"})},
	}

	scr := &datasource.Schema{
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
	gbkr1 := make(map[*datasource.Key][]*datasource.GenericColumnValue)
	gbkr1[scr.PrimaryKey] = []*datasource.GenericColumnValue{
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "id0",
		},
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "name0",
		},
	}
	gbkr2 := make(map[*datasource.Key][]*datasource.GenericColumnValue)
	gbkr2[scr.PrimaryKey] = []*datasource.GenericColumnValue{
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "id1",
		},
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "name1",
		},
	}
	gbkr3 := make(map[*datasource.Key][]*datasource.GenericColumnValue)
	gbkr3[scr.PrimaryKey] = []*datasource.GenericColumnValue{
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "id2",
		},
		{
			Column: &datasource.Column{Type: datasource.ColumnTypeString},
			Value:  "name2",
		},
	}
	gbkr4 := make(map[*datasource.Key][]*datasource.GenericColumnValue)
	gbkr4[scr.PrimaryKey] = []*datasource.GenericColumnValue{
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
		{GroupByKey: gbkr1, Values: newRowValuesFromString(map[string]string{"id": "id0", "name": "name0", "comment": "hello, world!"})},
		{GroupByKey: gbkr2, Values: newRowValuesFromString(map[string]string{"id": "id1", "name": "name1", "comment": "hello, world."})},
		{GroupByKey: gbkr3, Values: newRowValuesFromString(map[string]string{"id": "id2", "name": "name2", "comment": "hello, world!!!"})},
		{GroupByKey: gbkr4, Values: newRowValuesFromString(map[string]string{"id": "id3", "name": "name3", "comment": "hello, world!!!!"})},
	}

	differ, err := NewDiffer()
	if err != nil {
		t.Fatal(err)
	}

	// the same (no diff)
	{
		diff, err := differ.DiffRows(scl, scl, leftRows, leftRows)
		if err != nil {
			t.Fatal(err)
		}

		if len(diff.Left) != 0 || len(diff.Right) != 0 {
			t.Fatalf("expect: no row in diff, actual: diff.Left: %d, diff.Right: %d", len(diff.Left), len(diff.Right))
		}
	}

	{
		diff, err := differ.DiffRows(scl, scr, leftRows, rightRows)
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
	scl, err := ds.GetSchema(ctx, "")
	if err != nil {
		t.Fatal(err)
	}

	leftRows, err := ds.GetRows(ctx, scl)
	if err != nil {
		t.Fatal(err)
	}

	// Change column type string -> datetime
	for i, col := range scl.Columns {
		if col.Name == "birthday" {
			scl.Columns[i].Type = datasource.ColumnTypeDatetime
		}
	}

	scr, err := ds.GetSchema(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	rightRows, err := ds.GetRows(ctx, scr)
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

	diff, err := differ.DiffRows(scl, scr, leftRows, rightRows)
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
