package datasource

import (
	"context"
	"fmt"
	"testing"
)

func TestMockDatasource_Get(t *testing.T) {
	ds, err := NewMockDatasource()
	if err != nil {
		t.Fatal(err)
	}

	ctx := context.Background()
	sc, err := ds.GetSchema(ctx, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", sc)

	rows, err := ds.GetRows(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}

	for i, row := range rows {
		if row.Values["id"].Value != fmt.Sprintf("id%d", i) {
			t.Fatalf("rows[%d].Values['id'] must be 'id%d', but actual: %s", i, i, row.Values["id"].Value)
		}
		if i%2 == 0 && !row.Values["id"].Nullable {
			t.Fatalf("Even number of row index AND values id must nullable, but actually not null")
		}
		if row.Values["name"].Value != fmt.Sprintf("name%d", i) {
			t.Fatalf("rows[%d].Values['id'] must be 'name%d', but actual: %s", i, i, row.Values["name"].Value)
		}
		if i%2 != 0 && !row.Values["name"].Nullable {
			t.Fatalf("Odd number of row index AND values name must nullable, but actually not null")
		}
	}
}
