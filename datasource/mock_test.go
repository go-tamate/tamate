package datasource

import (
	"context"
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

	rows, err := ds.GetRows(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}

	for _, row := range rows.Values {
		t.Logf("%+v", row)
	}
}
