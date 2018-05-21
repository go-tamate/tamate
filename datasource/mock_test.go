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
		if row.values["id"].Value != fmt.Sprintf("id%d", i) {
			t.Fatalf("rows[%d].values['id'] must be 'id%s', but actual: %s", i, row.values["id"].Value)
		}
		if row.values["name"].Value != fmt.Sprintf("name%d", i) {
			t.Fatalf("rows[%d].values['id'] must be 'name%s', but actual: %s", i, row.values["name"].Value)
		}
	}
}
