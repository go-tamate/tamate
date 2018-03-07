package schema

import (
	"testing"
	"strings"
)

func TestNewJsonSchema(t *testing.T) {
	schemaJson := `
{
    "name": "Sample",
    "description": "サンプルテーブル",
    "table": {
        "name": "Sample",
        "primary_key": "id",
        "unique_key": []
    },
    "properties": [
        {
            "name": "id",
            "type": "int",
            "not_null": true,
            "auto_increment": true
        },
        {
            "name": "name",
            "type": "varchar(255)",
            "not_null": true
        },
        {
            "name": "age",
            "type": "int",
            "not_null": true
        },
        {
            "name": "created_at",
            "type": "datetime",
            "not_null": true
        }
    ]
}
`

	r := strings.NewReader(schemaJson)
	sc, err := NewJsonSchema(r)
	if err != nil {
		t.Fatal(err)
	}

	if sc.Name != "Sample" {
		t.Fatal("cannot read schema name")
	}
	if len(sc.Properties) != 4 {
		t.Fatalf("cannot read schema properties")
	}
}
