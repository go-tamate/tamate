package tamate

import (
	"testing"
)

func newRowValuesFromString(ss map[string]string) RowValues {
	res := make(RowValues, len(ss))
	for k, sv := range ss {
		res[k] = &GenericColumnValue{
			Column: &Column{Type: ColumnTypeString},
			Value:  sv,
		}
	}
	return res
}

func TestDiffer_DiffRows(t *testing.T) {

	scl := &Schema{
		Columns: []*Column{
			{Name: "id", Type: ColumnTypeString},
			{Name: "name", Type: ColumnTypeString},
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
		{GroupByKey: gbkl1, Values: newRowValuesFromString(map[string]string{"id": "id0", "name": "name0"})},
		{GroupByKey: gbkl2, Values: newRowValuesFromString(map[string]string{"id": "id1", "name": "name1"})},
	}

	scr := &Schema{
		Columns: []*Column{
			{Name: "id", Type: ColumnTypeString},
			{Name: "name", Type: ColumnTypeString},
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
	gbkr4 := make(GroupByKey)
	gbkr4[scr.PrimaryKey.String()] = []*GenericColumnValue{
		{
			Column: &Column{Type: ColumnTypeString},
			Value:  "id3",
		},
	}
	rightRows := []*Row{
		{GroupByKey: gbkr1, Values: newRowValuesFromString(map[string]string{"id": "id0", "name": "name0_modified"})},
		{GroupByKey: gbkr2, Values: newRowValuesFromString(map[string]string{"id": "id1", "name": "name1"})},
		{GroupByKey: gbkr3, Values: newRowValuesFromString(map[string]string{"id": "id2", "name": "name2"})},
		{GroupByKey: gbkr4, Values: newRowValuesFromString(map[string]string{"id": "id3", "name": "name3"})},
	}

	rowDiffer, err := newRowDiffer()
	if err != nil {
		t.Fatal(err)
	}

	// the same (no diff)
	{
		diff, err := rowDiffer.diff(scl, leftRows, leftRows)
		if err != nil {
			t.Fatal(err)
		}

		if len(diff.Left) != 0 || len(diff.Right) != 0 {
			t.Fatalf("expect: no row in diff, actual: diff.Left: %d, diff.Right: %d", len(diff.Left), len(diff.Right))
		}
	}

	{
		diff, err := rowDiffer.diff(scl, leftRows, rightRows)
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

	scl := &Schema{
		Columns: []*Column{
			{Name: "id", Type: ColumnTypeString},
			{Name: "name", Type: ColumnTypeString},
			{Name: "comment", Type: ColumnTypeString},
		},
		PrimaryKey: &Key{
			KeyType:     KeyTypePrimary,
			ColumnNames: []string{"id", "name"},
		},
	}
	gbkl1 := make(GroupByKey)
	gbkl1[scl.PrimaryKey.String()] = []*GenericColumnValue{
		{
			Column: &Column{Type: ColumnTypeString},
			Value:  "id0",
		},
		{
			Column: &Column{Type: ColumnTypeString},
			Value:  "name0",
		},
	}
	gbkl2 := make(GroupByKey)
	gbkl2[scl.PrimaryKey.String()] = []*GenericColumnValue{
		{
			Column: &Column{Type: ColumnTypeString},
			Value:  "id1",
		},
		{
			Column: &Column{Type: ColumnTypeString},
			Value:  "name1",
		},
	}
	leftRows := []*Row{
		{GroupByKey: gbkl1, Values: newRowValuesFromString(map[string]string{"id": "id0", "name": "name0", "comment": "hello, world!"})},
		{GroupByKey: gbkl2, Values: newRowValuesFromString(map[string]string{"id": "id1", "name": "name1", "comment": "hello, world!!"})},
	}

	scr := &Schema{
		Columns: []*Column{
			{Name: "id", Type: ColumnTypeString},
			{Name: "name", Type: ColumnTypeString},
			{Name: "comment", Type: ColumnTypeString},
		},
		PrimaryKey: &Key{
			KeyType:     KeyTypePrimary,
			ColumnNames: []string{"id", "name"},
		},
	}
	gbkr1 := make(GroupByKey)
	gbkr1[scr.PrimaryKey.String()] = []*GenericColumnValue{
		{
			Column: &Column{Type: ColumnTypeString},
			Value:  "id0",
		},
		{
			Column: &Column{Type: ColumnTypeString},
			Value:  "name0",
		},
	}
	gbkr2 := make(GroupByKey)
	gbkr2[scr.PrimaryKey.String()] = []*GenericColumnValue{
		{
			Column: &Column{Type: ColumnTypeString},
			Value:  "id1",
		},
		{
			Column: &Column{Type: ColumnTypeString},
			Value:  "name1",
		},
	}
	gbkr3 := make(GroupByKey)
	gbkr3[scr.PrimaryKey.String()] = []*GenericColumnValue{
		{
			Column: &Column{Type: ColumnTypeString},
			Value:  "id2",
		},
		{
			Column: &Column{Type: ColumnTypeString},
			Value:  "name2",
		},
	}
	gbkr4 := make(GroupByKey)
	gbkr4[scr.PrimaryKey.String()] = []*GenericColumnValue{
		{
			Column: &Column{Type: ColumnTypeString},
			Value:  "id3",
		},
		{
			Column: &Column{Type: ColumnTypeString},
			Value:  "name3",
		},
	}
	rightRows := []*Row{
		{GroupByKey: gbkr1, Values: newRowValuesFromString(map[string]string{"id": "id0", "name": "name0", "comment": "hello, world!"})},
		{GroupByKey: gbkr2, Values: newRowValuesFromString(map[string]string{"id": "id1", "name": "name1", "comment": "hello, world."})},
		{GroupByKey: gbkr3, Values: newRowValuesFromString(map[string]string{"id": "id2", "name": "name2", "comment": "hello, world!!!"})},
		{GroupByKey: gbkr4, Values: newRowValuesFromString(map[string]string{"id": "id3", "name": "name3", "comment": "hello, world!!!!"})},
	}

	rowDiffer, err := newRowDiffer()
	if err != nil {
		t.Fatal(err)
	}

	// the same (no diff)
	{
		diff, err := rowDiffer.diff(scl, leftRows, leftRows)
		if err != nil {
			t.Fatal(err)
		}

		if len(diff.Left) != 0 || len(diff.Right) != 0 {
			t.Fatalf("expect: no row in diff, actual: diff.Left: %d, diff.Right: %d", len(diff.Left), len(diff.Right))
		}
	}

	{
		diff, err := rowDiffer.diff(scl, leftRows, rightRows)
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

/*
func TestDiffer_DiffDatetimeFormatStringColumn(t *testing.T) {
	ds, err := NewMockDatasource()
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
			scl.Columns[i].Type = ColumnTypeDatetime
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

	rowDiffer, err := newRowDiffer()
	if err != nil {
		t.Fatal(err)
	}

	diff, err := rowDiffer.diff(scl, leftRows, rightRows)
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
*/
