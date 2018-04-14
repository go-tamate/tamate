package main

import (
	"bytes"
	"testing"
)

func TestGenerateTable(t *testing.T) {
	var b bytes.Buffer
	for _, tp := range []string{"sql", "csv", "spreadsheet"} {
		if err := generateTable(&b, tp); err != nil {
			t.Fatal(err)
		}
		t.Log(string(b.Bytes()))
	}
}
