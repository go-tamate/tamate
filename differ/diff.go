package differ

import (
	"encoding/json"

	"github.com/Mitu217/tamate/datasource"
)

type Diff struct {
	Schema      *datasource.Schema
	DiffColumns *DiffColumns
	DiffRows    *DiffRows
}

func (d *Diff) ExportJSON() ([]byte, error) {
	v := struct {
		Schema      *datasource.Schema `json:"schema"`
		DiffColumns *DiffColumns       `json:"diff_columns"`
		DIffRows    *DiffRows          `json:"diff_rows"`
	}{
		Schema:      d.Schema,
		DiffColumns: d.DiffColumns,
		DIffRows:    d.DiffRows,
	}
	return json.Marshal(v)
}

func (d *Diff) ExportPrettyJSON() ([]byte, error) {
	v := struct {
		Schema      *datasource.Schema `json:"schema"`
		DiffColumns *DiffColumns       `json:"diff_columns"`
		DIffRows    *DiffRows          `json:"diff_rows"`
	}{
		Schema:      d.Schema,
		DiffColumns: d.DiffColumns,
		DIffRows:    d.DiffRows,
	}
	return json.MarshalIndent(v, "", "  ")
}

func (d *Diff) ExportSQL() ([]byte, error) {
	v := `
		SHOW TABLES;
	`
	return []byte(v), nil
}
