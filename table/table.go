package table

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	"github.com/Mitu217/tamate/table/schema"
)

// Rows :
type Rows struct {
	Values [][]string
}

type Table interface {
	GetSchema() (*schema.Schema, error)
	GetRows() (*Rows, error)
}

type tableJSON struct {
	Type string `json:"type"`
	Data Table  `json:"data"`
}

func ToJSON(table Table, w io.Writer) error {
	typeName := reflect.TypeOf(table).Elem().Name()
	tj := &tableJSON{
		Type: typeName,
		Data: table,
	}
	enc := json.NewEncoder(w)
	return enc.Encode(tj)
}

func FromJSON(r io.Reader) (Table, error) {
	var tj struct {
		Type string `json:"type"`
		Data interface{}
	}
	dec := json.NewDecoder(r)
	if err := dec.Decode(&tj); err != nil {
		return nil, err
	}

	// ugly...
	var b bytes.Buffer
	enc := json.NewEncoder(&b)
	if err := enc.Encode(tj.Data); err != nil {
		return nil, err
	}
	dec2 := json.NewDecoder(&b)
	var tbl Table
	switch tj.Type {
	case "CSVTable":
		tbl = &CSVTable{}
		break
	case "SpreadsheetTable":
		tbl = &SpreadsheetTable{}
		break
	case "SQLTable":
		tbl = &SQLTable{}
	default:
		return nil, fmt.Errorf("invalid type: %s", tj.Type)
	}
	dec2.Decode(tbl)
	return tbl, nil
}
