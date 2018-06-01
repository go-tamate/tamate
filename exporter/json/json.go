package json

import (
	"context"

	"encoding/json"

	"github.com/Mitu217/tamate/datasource"
	"github.com/Mitu217/tamate/differ"
	"github.com/Mitu217/tamate/exporter"
)

type JSONExporter struct {
	leftDatasource  datasource.Datasource
	rightDatasource datasource.Datasource
	leftTargetName  string
	rightTargetName string
	diffDir         exporter.DiffDirection
	pretty          bool
}

func NewExporter(leftDs, rightDs datasource.Datasource, leftName, rightName string) *JSONExporter {
	return &JSONExporter{
		leftDatasource:  leftDs,
		rightDatasource: rightDs,
		leftTargetName:  leftName,
		rightTargetName: rightName,
		diffDir:         exporter.DiffDirectionLeftToRight,
		pretty:          false,
	}
}

func (je *JSONExporter) SetPretty(pretty bool) {
	je.pretty = pretty
}

func (je *JSONExporter) SetDirection(direction exporter.DiffDirection) {
	je.diffDir = direction
}

func (je *JSONExporter) SetDatasources(left, right datasource.Datasource) {
	je.leftDatasource = left
	je.rightDatasource = right
}

func (je *JSONExporter) ExportStruct() (*differ.Diff, error) {

	ctx := context.Background()
	df, err := differ.NewDiffer()
	if err != nil {
		return nil, err
	}

	leftSchema, err := je.leftDatasource.GetSchema(ctx, je.leftTargetName)
	if err != nil {
		return nil, err
	}
	leftRows, err := je.leftDatasource.GetRows(ctx, leftSchema)
	if err != nil {
		return nil, err
	}

	rightSchema, err := je.rightDatasource.GetSchema(ctx, je.rightTargetName)
	if err != nil {
		return nil, err
	}
	rightRows, err := je.rightDatasource.GetRows(ctx, rightSchema)
	if err != nil {
		return nil, err
	}

	var diffRows *differ.DiffRows
	var diffRowsError error
	if je.diffDir == exporter.DiffDirectionLeftToRight {
		diffRows, diffRowsError = df.DiffRows(leftSchema, rightSchema, leftRows, rightRows)
	}
	if je.diffDir == exporter.DiffDirectionRightToLeft {
		diffRows, diffRowsError = df.DiffRows(rightSchema, leftSchema, rightRows, leftRows)
	}
	if diffRowsError != nil {
		return nil, diffRowsError
	}

	var diffColumns *differ.DiffColumns
	var diffColumnsError error
	if je.diffDir == exporter.DiffDirectionLeftToRight {
		diffColumns, diffColumnsError = df.DiffColumns(leftSchema, rightSchema)
	}
	if je.diffDir == exporter.DiffDirectionRightToLeft {
		diffColumns, diffColumnsError = df.DiffColumns(rightSchema, leftSchema)
	}
	if diffColumnsError != nil {
		return nil, diffColumnsError
	}

	diff := &differ.Diff{
		Schema:  leftSchema,
		Columns: diffColumns,
		Rows:    diffRows,
	}

	return diff, nil

}

func (je *JSONExporter) ExportJSON() ([]byte, error) {

	diff, err := je.ExportStruct()
	if err != nil {
		return nil, err
	}

	var b []byte
	var marshalError error
	if je.pretty {
		b, marshalError = json.MarshalIndent(diff, "", "  ")
	} else {
		b, marshalError = json.Marshal(diff)
	}

	return b, marshalError

}
