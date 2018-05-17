package handler

import (
	"os"
	"testing"
)

func TestSpannerHandler_GetRows(t *testing.T) {
	h, err := NewSpannerHandler(os.Getenv("TAMATE_SPANNER_DSN"))
	if err != nil {
		t.Fatal(err)
	}

	scs, err := h.GetSchemas()
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("%+v", scs[0])

	rows, err := h.GetRows(scs[0])
	if err != nil {
		t.Fatal(err)
	}
	t.Log(rows.Values)
}
