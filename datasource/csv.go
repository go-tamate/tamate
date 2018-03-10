package datasource

import (
	"encoding/csv"
	"os"

	"github.com/Mitu217/tamate/config"
	"github.com/Mitu217/tamate/schema"
)

// CSVDataSource :
type CSVDataSource struct {
	Config *config.CSVConfig
	Schema schema.Schema
}

// NewCSVDataSource :
func NewCSVDataSource(sc schema.Schema, config *config.CSVConfig) (*CSVDataSource, error) {
	ds := &CSVDataSource{
		Config: config,
		Schema: sc,
	}
	return ds, nil
}

// GetSchema :
func (ds *CSVDataSource) GetSchema() (schema.Schema, error) {
	return ds.Schema, nil
}

// GetRows :
func (ds *CSVDataSource) GetRows() (*Rows, error) {
	r, err := os.Open(ds.Config.Path)
	if err != nil {
		return nil, err
	}

	records, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, err
	}

	// FIXME: columnsはSchemaを正とするようにデータを生成する
	columns := records[0]
	values := append(records[:0], records[1:]...)
	rows := &Rows{
		Columns: columns,
		Values:  values,
	}
	return rows, nil
}

// SetRows :
func (ds *CSVDataSource) SetRows(rows *Rows) error {
	file, err := os.Create(ds.Config.Path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, value := range append([][]string{rows.Columns}, rows.Values...) {
		err := writer.Write(value)
		if err != nil {
			return err
		}
	}
	return nil
}
