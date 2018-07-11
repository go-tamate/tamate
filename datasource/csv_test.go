package datasource

import (
	"context"
	"testing"
)

const TestRootPath = "../examples/csv"
const TestFileName = "example1"
const TestColumnRowIndex = 0

const (
	IndexID = iota
	IndexName
	IndexAge
)

func TestCSVDatasource_Get(t *testing.T) {
	ctx := context.Background()
	csvValues, err := readFromFile(TestRootPath, TestFileName)
	if err != nil {
		t.Fatal(err)
	}

	ds, err := NewCSVDatasource(TestRootPath, TestColumnRowIndex)
	if err != nil {
		t.Fatal(err)
	}

	sc, err := ds.GetSchema(ctx, TestFileName)
	if err != nil {
		t.Fatal(err)
	}
	if sc.Columns[IndexID].Name != "id" {
		t.Fatalf("sc.Columns[%d].Name must be %+v, but actual: %+v", IndexID, "id", sc.Columns[IndexID].Name)
	}
	if sc.Columns[IndexName].Name != "name" {
		t.Fatalf("sc.Columns[%d].Name must be %+v, but actual: %+v", IndexName, "name", sc.Columns[IndexName].Name)
	}
	if sc.Columns[IndexAge].Name != "age" {
		t.Fatalf("sc.Columns[%d].Name must be %+v, but actual: %+v", IndexAge, "age", sc.Columns[IndexAge].Name)
	}

	rows, err := ds.GetRows(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}
	for i, row := range rows {
		csvIndex := i
		if csvIndex >= ds.ColumnRowIndex {
			csvIndex++
		}
		if row.Values["id"].Value != csvValues[csvIndex][IndexID] {
			t.Fatalf("rows[%d].Values['id'] must be %+v, but actual: %+v", i, row.Values["id"].Value, csvValues[csvIndex][IndexID])
		}
		if row.Values["name"].Value != csvValues[csvIndex][IndexName] {
			t.Fatalf("rows[%d].Values['name'] must be %+v, but actual: %+v", i, row.Values["name"].Value, csvValues[csvIndex][IndexName])
		}
		if row.Values["age"].Value != csvValues[csvIndex][IndexAge] {
			t.Fatalf("rows[%d].Values['age'] must be %+v, but actual: %+v", i, row.Values["age"].Value, csvValues[csvIndex][IndexAge])
		}
	}
}

func TestCSVDatasource_Set(t *testing.T) {
	t.Skip()
}
