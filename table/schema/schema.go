package schema

// Column :
type Column struct {
	Name          string `json:"name"`
	Type          string `json:"type"`
	NotNull       bool   `json:"not_null"`
	AutoIncrement bool   `json:"auto_increment"`
}

// schema :
type Schema struct {
	Name       string   `json:"name"`
	PrimaryKey string   `json:"primary_key"`
	Columns    []Column `json:"properties"`
}

func (sc *Schema) HasColumn(name string) bool {
	for _, col := range sc.Columns {
		if col.Name == name {
			return true
		}
	}
	return false
}

// All of type is as "string"
func NewSchemaFromRow(tableName string, row []string) (*Schema, error) {
	var cols []Column
	for _, colName := range row {
		cols = append(cols, Column{
			Name:          colName,
			Type:          "string",
			NotNull:       true,
			AutoIncrement: false,
		})
	}
	return &Schema{
		Name:    tableName,
		Columns: cols,
	}, nil
}
