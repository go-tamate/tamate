package datasource

import (
	"context"
	"log"
	"os"
	"strings"
	"testing"
)

const (
	TestCSVRootPath       = "./"
	TestCSVFileName       = "sample"
	TestCSVColumnRowIndex = 0
)

var testData = `
	(id),name,age
	1,hana,16
	2,tamate,15
	3,kamuri,15
	4,eiko,15
`

const (
	TestCSVIndexID = iota
	TestCSVIndexName
	TestCSVIndexAge
)

func setupCSVDatasourceTest(t *testing.T) (func(), error) {
	r := strings.NewReader(testData)
	csvValues, err := read(r)
	if err != nil {
		return nil, err
	}
	if err := writeToFile(TestCSVRootPath, TestCSVFileName, csvValues); err != nil {
		return nil, err
	}
	return func() {
		if err := os.Remove(TestCSVRootPath + TestCSVFileName + ".csv"); err != nil {
			log.Println(err)
		}
	}, nil
}

func TestCSVDatasource_Get(t *testing.T) {
	TearDown, err := setupCSVDatasourceTest(t)
	if err != nil {
		t.Fatal(err)
	}
	defer TearDown()

	ctx := context.Background()

	r := strings.NewReader(testData)
	csvValues, err := read(r)
	if err != nil {
		t.Fatal(err)
	}

	ds, err := NewCSVDatasource(TestCSVRootPath, TestCSVColumnRowIndex)
	if err != nil {
		t.Fatal(err)
	}

	sc, err := ds.GetSchema(ctx, TestCSVFileName)
	if err != nil {
		t.Fatal(err)
	}
	if sc.Columns[TestCSVIndexID].Name != "id" {
		t.Fatalf("sc.Columns[%d].Name must be %+v, but actual: %+v", TestCSVIndexID, "id", sc.Columns[TestCSVIndexID].Name)
	}
	if sc.Columns[TestCSVIndexName].Name != "name" {
		t.Fatalf("sc.Columns[%d].Name must be %+v, but actual: %+v", TestCSVIndexName, "name", sc.Columns[TestCSVIndexName].Name)
	}
	if sc.Columns[TestCSVIndexAge].Name != "age" {
		t.Fatalf("sc.Columns[%d].Name must be %+v, but actual: %+v", TestCSVIndexAge, "age", sc.Columns[TestCSVIndexAge].Name)
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
		if row.Values["id"].Value != csvValues[csvIndex][TestCSVIndexID] {
			t.Fatalf("rows[%d].Values['id'] must be %+v, but actual: %+v", i, row.Values["id"].Value, csvValues[csvIndex][TestCSVIndexID])
		}
		if row.Values["name"].Value != csvValues[csvIndex][TestCSVIndexName] {
			t.Fatalf("rows[%d].Values['name'] must be %+v, but actual: %+v", i, row.Values["name"].Value, csvValues[csvIndex][TestCSVIndexName])
		}
		if row.Values["age"].Value != csvValues[csvIndex][TestCSVIndexAge] {
			t.Fatalf("rows[%d].Values['age'] must be %+v, but actual: %+v", i, row.Values["age"].Value, csvValues[csvIndex][TestCSVIndexAge])
		}
	}
}
