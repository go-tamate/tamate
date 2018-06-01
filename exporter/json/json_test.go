package json

import (
	"testing"

	"encoding/json"

	"github.com/Mitu217/tamate/datasource"
	"github.com/Mitu217/tamate/differ"
	"github.com/Mitu217/tamate/exporter"
)

func TestJSONExporter_ExportJSON(t *testing.T) {

	mockLeft, err := datasource.NewMockDatasource()
	if err != nil {
		t.Fatal(err)
	}
	mockRight, err := datasource.NewMockDatasource()
	if err != nil {
		t.Fatal(err)
	}

	jsonExporter := JSONExporter{
		LeftTargetName:  "",
		RightTargetName: "",
	}
	b, err := jsonExporter.ExportJSON(mockLeft, mockRight)
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

	jsonExporter := JSONExporter{
		LeftTargetName:  "",
		RightTargetName: "",
	}

	_, err = jsonExporter.ExportStruct(mockLeft, mockRight)
	if err != nil {
		t.Fatal(err)
	}

}

func TestJSONExporter_SetDirection(t *testing.T) {

	jsonExporter := JSONExporter{
		LeftTargetName:  "",
		RightTargetName: "",
	}

	if jsonExporter.diffDir != exporter.DiffDirectionLeftToRight {
		t.Fatalf("Expect default to LEFT_TO_RIGHT direction but actual (%s).", jsonExporter.diffDir)
	}

	jsonExporter.SetDirection(exporter.DiffDirectionRightToLeft)
	if jsonExporter.diffDir != exporter.DiffDirectionRightToLeft {
		t.Fatalf("Expect RIGHT_TO_LEFT direction but actual (%s).", jsonExporter.diffDir)
	}

	jsonExporter.SetDirection(exporter.DiffDirectionLeftToRight)
	if jsonExporter.diffDir != exporter.DiffDirectionLeftToRight {
		t.Fatalf("Expect LEFT_TO_RIGHT direction but actual (%s).", jsonExporter.diffDir)
	}

}
