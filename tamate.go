package tamate

import (
	"context"

	"github.com/Mitu217/tamate/datasource"
	"github.com/Mitu217/tamate/differ"
)

// Diff is return diff between left and right datasources
func Diff(ctx context.Context, lds, rds datasource.Datasource, leftSchemaName, rightSchemaName string) (*differ.Diff, error) {
	leftSchema, err := lds.GetSchema(ctx, leftSchemaName)
	if err != nil {
		return nil, err
	}
	leftRows, err := lds.GetRows(ctx, leftSchema)
	if err != nil {
		return nil, err
	}
	rightSchema, err := rds.GetSchema(ctx, rightSchemaName)
	if err != nil {
		return nil, err
	}
	rightRows, err := rds.GetRows(ctx, rightSchema)
	if err != nil {
		return nil, err
	}

	d, err := differ.NewDiffer()
	if err != nil {
		return nil, err
	}
	diffColumns, err := d.DiffColumns(leftSchema, rightSchema)
	if err != nil {
		return nil, err
	}
	diffRows, err := d.DiffRows(leftSchema, leftRows, rightRows)
	if err != nil {
		return nil, err
	}
	return &differ.Diff{
		Schema:      leftSchema,
		DiffColumns: diffColumns,
		DiffRows:    diffRows,
	}, nil
}
