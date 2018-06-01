package json

import (
	"context"

	"encoding/json"

	"github.com/Mitu217/tamate/datasource"
	"github.com/Mitu217/tamate/differ"
	"github.com/Mitu217/tamate/exporter"
)

type JSONExporter struct {
	LeftTargetName  string
	RightTargetName string
	diffDir         exporter.DiffDirection
	Pretty          bool
}

func (je *JSONExporter) SetDirection(direction exporter.DiffDirection) {
	je.diffDir = direction
}

func (je *JSONExporter) ExportStruct(l datasource.Datasource, r datasource.Datasource) (*differ.Diff, error) {

	ctx := context.Background()
	df, err := differ.NewDiffer()
	if err != nil {
		return nil, err
	}

	leftSchema, err := l.GetSchema(ctx, je.LeftTargetName)
	if err != nil {
		return nil, err
	}
	leftRows, err := l.GetRows(ctx, leftSchema)
	if err != nil {
		return nil, err
	}

	rightSchema, err := r.GetSchema(ctx, je.RightTargetName)
	if err != nil {
		return nil, err
	}
	rightRows, err := r.GetRows(ctx, rightSchema)
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

func (je *JSONExporter) ExportJSON(l datasource.Datasource, r datasource.Datasource) ([]byte, error) {

	diff, err := je.ExportStruct(l, r)
	if err != nil {
		return nil, err
	}

	var b []byte
	var marshalError error
	if je.Pretty {
		b, marshalError = json.MarshalIndent(diff, "", "  ")
	} else {
		b, marshalError = json.Marshal(diff)
	}

	return b, marshalError

}
