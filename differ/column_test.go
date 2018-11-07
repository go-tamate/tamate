package differ

import (
	"testing"

	"github.com/Mitu217/tamate/datasource"
)

func TestDiffer_DiffColumns(t *testing.T) {
	columnDiffer, err := newColumnDiffer()
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

		d, err := columnDiffer.diff(left, right)
		if err != nil {
			t.Fatal(err)
		}

		if len(d.Left) != 1 || len(d.Right) != 1 {
			t.Fatalf("expect: 1 columns modified, actual: left: %d, right: %d", len(d.Left), len(d.Right))
		}
	}
}
