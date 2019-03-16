package differ

import (
	"testing"

	"github.com/go-tamate/tamate/driver"
)

func TestDiffer_DiffColumns(t *testing.T) {
	columnDiffer := newColumnDiffer()

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
		},
		PrimaryKey: &driver.Key{ColumnNames: []string{"id"}},
	}

	d, err := columnDiffer.diff(left, right)
	if err != nil {
		t.Fatal(err)
	}

	if len(d.Left) != 1 || len(d.Right) != 1 {
		t.Fatalf("expect: 1 columns modified, actual: left: %d, right: %d", len(d.Left), len(d.Right))
	}
}
