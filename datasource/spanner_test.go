package datasource

import (
	"context"
	"os"
	"testing"
)

func TestSpanner_GetRows(t *testing.T) {
	dsn := os.Getenv("TAMATE_SPANNER_DSN")
	if dsn == "" {
		t.Skip("env: TAMATE_SPANNER_DSN not set")
	}

	h, err := NewSpannerDatasource(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer h.Close()

	ctx := context.Background()
	sc, err := h.GetSchema(ctx, "User")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Schema: %+v", sc)
	t.Logf("PK: %+v", sc.PrimaryKey)

	rows, err := h.GetRows(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(rows.Values)
}
