package tamate

import (
	"context"

	"github.com/Mitu217/tamate/datasource"
	"github.com/Mitu217/tamate/differ"
)

func Diff(ctx context.Context, lds, rds datasource.Datasource, leftSchemaName, rightSchemaName string) (*differ.DiffColumns, *differ.DiffRows, error) {
	leftSchema, err := lds.GetSchema(ctx, leftSchemaName)
	if err != nil {
		return nil, nil, err
	}
	leftRows, err := lds.GetRows(ctx, leftSchema)
	if err != nil {
		return nil, nil, err
	}
	rightSchema, err := rds.GetSchema(ctx, rightSchemaName)
	if err != nil {
		return nil, nil, err
	}
	rightRows, err := rds.GetRows(ctx, rightSchema)
	if err != nil {
		return nil, nil, err
	}

	d, err := differ.NewDiffer()
	if err != nil {
		return nil, nil, err
	}
	dColumns, err := d.DiffColumns(leftSchema, rightSchema)
	if err != nil {
		return nil, nil, err
	}
	dRows, err := d.DiffRows(leftSchema, leftRows, rightRows)
	if err != nil {
		return nil, nil, err
	}
	return dColumns, dRows, nil
}
