package datasource

import (
	"encoding/csv"
	"os"

	"github.com/Mitu217/tamate/schema"
)

// CSVConfig :
type CSVConfig struct {
	SoursePath string
	OutputPath string
}

// CSVDataSource :
type CSVDataSource struct {
	Config *CSVConfig
	Schema schema.Schema
}

// NewCSVConfig :
func NewCSVConfig(srcPath string, dstPath string) *CSVConfig {
	config := &CSVConfig{
		SoursePath: srcPath,
		OutputPath: dstPath,
	}
	return config
}

// NewCSVDataSource :
func NewCSVDataSource(sc schema.Schema, config *CSVConfig) (*CSVDataSource, error) {
	ds := &CSVDataSource{
		Config: config,
		Schema: sc,
	}
	return ds, nil
}

// GetSchema :
func (ds *CSVDataSource) GetSchema() schema.Schema {
	return ds.Schema
}

// GetRows :
func (ds *CSVDataSource) GetRows() (*Rows, error) {
	r, err := os.Open(ds.Config.SoursePath)
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
	file, err := os.Create(ds.Config.OutputPath)
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
