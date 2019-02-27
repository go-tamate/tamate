package tamate

import (
	"testing"
)

func TestDiffer_DiffColumns(t *testing.T) {
	columnDiffer, err := newColumnDiffer()
	if err != nil {
		t.Fatal(err)
	}

	{
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
			},
			PrimaryKey: &Key{ColumnNames: []string{"id"}},
		}

		d, err := columnDiffer.diff(left, right)
		if err != nil {
			t.Fatal(err)
		}

		if len(d.Left) != 1 || len(d.Right) != 1 {
			t.Fatalf("expect: 1 columns modified, actual: left: %d, right: %d", len(d.Left), len(d.Right))
		}
	}
}
