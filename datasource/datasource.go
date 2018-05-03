package datasource

import (
	"encoding/json"
	"errors"
	"io"
	"strings"

	"github.com/Mitu217/tamate/datasource/handler"
)

// Table is table struct in datasource
type Table struct {
	Schema handler.Schema `json:"schema"`
	rows   handler.Rows
}

// Datasource is datasource interface
type Datasource struct {
	Type    string
	tables  []Table
	handler handler.Handler
}

// ToJSON is datasource to json method
func ToJSON(ds *Datasource, w io.Writer) error {
	//typeName := reflect.TypeOf(config).Elem().Name()
	//enc := json.NewEncoder(w)

	// TODO: typeNameごとにEncodeを行う

	return nil //enc.Encode(tj)
}

// FromJSON is json to datasource method
func FromJSON(r io.Reader) (*Datasource, error) {
	// decode JSON
	var ds struct {
		Type   string      `json:"type"`
		Config interface{} `json:"config"`
		Tables []Table     `json:"table"`
	}
	if err := json.NewDecoder(r).Decode(&ds); err != nil {
		return nil, err
	}
	// decode HandlerConfig
	var h handler.Handler
	switch ds.Type {
	case CSV.String():
		h = &handler.CSVHandler{}
		break
	case Spreadsheet.String():
		h = &handler.SpreadsheetHandler{}
		break
	case SQL.String():
		h = &handler.SQLHandler{}
		break
	default:
		return nil, errors.New("invalid type: " + ds.Type)
	}
	configBytes, err := json.Marshal(ds.Config)
	if err != nil {
		return nil, err
	}
	if err := json.NewDecoder(strings.NewReader(string(configBytes))).Decode(h); err != nil {
		return nil, err
	}
	return &Datasource{
		Type:    ds.Type,
		tables:  ds.Tables,
		handler: h,
	}, nil
}

// Open is create handler method
func (ds *Datasource) Open() error {
	return ds.handler.Open()
}

// Close is free handler method
func (ds *Datasource) Close() error {
	return ds.handler.Close()
}

// GetSchemas is get all schema method
func (ds *Datasource) GetSchemas() (*[]handler.Schema, error) {
	if ds.handler == nil {
		return nil, errors.New("not open")
	}
	return ds.handler.GetSchemas()
}

// GetSchema is get schema from datasource method
func (ds *Datasource) GetSchema(schemaName string) (*handler.Schema, error) {
	if ds.handler == nil {
		return nil, errors.New("not open")
	}
	tables, err := ds.getTables()
	if err != nil {
		return nil, errors.New("not define schemas")
	}
	for _, table := range tables {
		if table.Schema.Name == schemaName {
			return &table.Schema, nil
		}
	}
	return nil, errors.New("not found schema: " + schemaName)
}

// SetSchema is set schema to datasource method
func (ds *Datasource) SetSchema(schema *handler.Schema) error {
	if ds.handler == nil {
		return errors.New("not open")
	}
	tables, err := ds.getTables()
	if err != nil {
		return errors.New("not define schemas")
	}
	for _, table := range tables {
		if table.Schema.Name == schema.Name {
			return ds.handler.SetSchema(schema)
		}
	}
	return errors.New("not found schema: " + schema.Name)
}

// GetRows is get rows from datasource method
func (ds *Datasource) GetRows(schemaName string) (*handler.Rows, error) {
	if ds.handler == nil {
		return nil, errors.New("not open")
	}
	tables, err := ds.getTables()
	if err != nil {
		return nil, errors.New("not define schemas")
	}
	for _, table := range tables {
		if table.Schema.Name == schemaName {
			return ds.handler.GetRows(&table.Schema)
		}
	}
	return nil, errors.New("not found schema: " + schemaName)
}

// SetRows is set rows to datasource method
func (ds *Datasource) SetRows(schemaName string, rows *handler.Rows) error {
	if ds.handler == nil {
		return errors.New("not open")
	}
	tables, err := ds.getTables()
	if err != nil {
		return err
	}
	for _, table := range tables {
		if table.Schema.Name == schemaName {
			return ds.handler.SetRows(&table.Schema, rows)
		}
	}
	return errors.New("not found schema: " + schemaName)
}

func (ds *Datasource) getTables() ([]Table, error) {
	if ds.tables == nil {
		schemas, err := ds.handler.GetSchemas()
		if err != nil {
			return nil, err
		}
		if schemas == nil {
			return nil, errors.New("not define schemas")
		}
		tables := make([]Table, len(*schemas))
		for i, schema := range *schemas {
			table := Table{
				Schema: schema,
			}
			tables[i] = table
		}
		ds.tables = tables
	}
	return ds.tables, nil
}
