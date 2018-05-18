package differ

import (
	"github.com/Mitu217/tamate/datasource"
	"testing"
)

func TestDiffer_DiffRows(t *testing.T) {
	sc := &datasource.Schema{
		Columns: []*datasource.Column{
			{Name: "id", Type: "string"},
		},
		PrimaryKey: &datasource.PrimaryKey{ColumnNames: []string{"id"}},
	}

	left := &datasource.Rows{
		Values: [][]string{
			{"id0", "name0"},
			{"id1", "name1"},
		},
	}
	right := &datasource.Rows{
		Values: [][]string{
			{"id0", "name00"},
			{"id1", "name1"},
			{"id2", "name2"},
			{"id3", "name3"},
		},
	}

	differ, err := NewDiffer()
	if err != nil {
		t.Fatal(err)
	}

	{
		diff, err := differ.DiffRows(sc, left, right)
		if err != nil {
			t.Fatal(err)
		}

		if len(diff.Add) != 2 {
			t.Fatalf("expected: 2 rows added, actual: %d rows added", len(diff.Add))
		}
		if len(diff.Modify) != 1 {
			t.Fatalf("expected: 1 rows modified, actual: %d rows modified", len(diff.Modify))
		}
		if diff.Modify[0].Right[1] != "name00" {
			t.Fatalf("modified name must be 'name00'")
		}
		if len(diff.Delete) > 0 {
			t.Fatalf("expected: no rows deleted, actual: %d rows deleted", len(diff.Delete))
		}
	}

	// 逆方向
	{
		diff2, err := differ.DiffRows(sc, right, left)
		if err != nil {
			t.Fatal(err)
		}
		if len(diff2.Delete) != 2 {
			t.Fatalf("expected: 2 rows deleted, actual: %d rows deleted", len(diff2.Delete))
		}
		if len(diff2.Modify) != 1 {
			t.Fatalf("expected: 1 rows modified, actual: modified %d rows", len(diff2.Modify))
		}
		if diff2.Modify[0].Right[1] != "name0" {
			t.Fatalf("modified name must be 'name0'")
		}
		if len(diff2.Add) > 0 {
			t.Fatalf("expected: no rows added, actual: %d rows added", len(diff2.Add))
		}
	}
}
