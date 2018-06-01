package json

import (
	"testing"

	"encoding/json"

	"github.com/Mitu217/tamate/datasource"
	"github.com/Mitu217/tamate/differ"
	"github.com/Mitu217/tamate/exporter"
)

func TestNewExporter(t *testing.T) {

	mockLeft, err := datasource.NewMockDatasource()
	if err != nil {
		t.Fatal(err)
	}
	mockRight, err := datasource.NewMockDatasource()
	if err != nil {
		t.Fatal(err)
	}
	jsonExporter := NewExporter(mockLeft, mockRight, "", "")

	if jsonExporter.diffDir != exporter.DiffDirectionLeftToRight {
		t.Fatalf("Expect default to LEFT_TO_RIGHT direction but actual (%s).", jsonExporter.diffDir)
	}
	if jsonExporter.pretty {
		t.Fatalf("Expect default to false but actual (%v).", jsonExporter.pretty)
	}

}

func TestJSONExporter_SetDatasources(t *testing.T) {
	mockLeft, err := datasource.NewMockDatasource()
	if err != nil {
		t.Fatal(err)
	}
	mockRight, err := datasource.NewMockDatasource()
	if err != nil {
		t.Fatal(err)
	}
	jsonExporter := JSONExporter{}
	jsonExporter.SetDatasources(mockLeft, mockRight)

	if jsonExporter.leftDatasource == nil || jsonExporter.rightDatasource == nil {
		t.Fatal("Expect datasources to be set, but actual nil.")
	}
}

func TestJSONExporter_SetPretty(t *testing.T) {
	jsonExporter := JSONExporter{}
	jsonExporter.SetPretty(true)
	if !jsonExporter.pretty {
		t.Fatal("Expect pretty option to be true, but actual false.")
	}
	jsonExporter.SetPretty(false)
	if jsonExporter.pretty {
		t.Fatal("Expect pretty option to be false, but actual true.")
	}
}

func TestJSONExporter_ExportJSON(t *testing.T) {

	mockLeft, err := datasource.NewMockDatasource()
	if err != nil {
		t.Fatal(err)
	}
	mockRight, err := datasource.NewMockDatasource()
	if err != nil {
		t.Fatal(err)
	}

	jsonExporter := NewExporter(mockLeft, mockRight, "", "")
	b, err := jsonExporter.ExportJSON()
	if err != nil {
		t.Fatal(err)
	}

	var diff differ.Diff

	err = json.Unmarshal(b, &diff)
	if err != nil {
		t.Fatal(err)
	}
}

func TestJSONExporter_ExportStruct(t *testing.T) {
	mockLeft, err := datasource.NewMockDatasource()
	if err != nil {
		t.Fatal(err)
	}
	mockRight, err := datasource.NewMockDatasource()
	if err != nil {
		t.Fatal(err)
	}

	jsonExporter := NewExporter(mockLeft, mockRight, "", "")

	_, err = jsonExporter.ExportStruct()
	if err != nil {
		t.Fatal(err)
	}

}

func TestJSONExporter_SetDirection(t *testing.T) {

	mockLeft, err := datasource.NewMockDatasource()
	if err != nil {
		t.Fatal(err)
	}
	mockRight, err := datasource.NewMockDatasource()
	if err != nil {
		t.Fatal(err)
	}
	jsonExporter := NewExporter(mockLeft, mockRight, "", "")

	jsonExporter.SetDirection(exporter.DiffDirectionRightToLeft)
	if jsonExporter.diffDir != exporter.DiffDirectionRightToLeft {
		t.Fatalf("Expect RIGHT_TO_LEFT direction but actual (%s).", jsonExporter.diffDir)
	}

	jsonExporter.SetDirection(exporter.DiffDirectionLeftToRight)
	if jsonExporter.diffDir != exporter.DiffDirectionLeftToRight {
		t.Fatalf("Expect LEFT_TO_RIGHT direction but actual (%s).", jsonExporter.diffDir)
	}

}
