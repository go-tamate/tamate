package datasource

import (
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

	sc, err := h.GetSchema("User")
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Schema: %+v", sc)
	t.Logf("PK: %+v", sc.PrimaryKey)

	rows, err := h.GetRows(sc)
	if err != nil {
		t.Fatal(err)
	}
	t.Log(rows.Values)
}
