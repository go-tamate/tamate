package differ

import (
	"encoding/json"

	"github.com/Mitu217/tamate/datasource"
)

type Diff struct {
	Schema      *datasource.Schema `json:"schema"`
	DiffColumns *DiffColumns       `json:"diff_columns"`
	DiffRows    *DiffRows          `json:"diff_rows"`
}

func (d *Diff) HasDiff() bool {
	return d.DiffColumns.HasDiff() || d.DiffRows.HasDiff()
}

func (d *Diff) ExportJSON() ([]byte, error) {
	return json.Marshal(d)
}

func (d *Diff) ExportPrettyJSON() ([]byte, error) {
	return json.MarshalIndent(d, "", "  ")
}

func (d *Diff) ExportSQL() ([]byte, error) {
	v := `
		SHOW TABLES;
	`
	return []byte(v), nil
}
