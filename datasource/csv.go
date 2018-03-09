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
	Rows   *Rows
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
	if config.SoursePath == "" {
		ds := &CSVDataSource{
			Config: config,
		}
		return ds, nil
	}

	r, err := os.Open(config.SoursePath)
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
	ds := &CSVDataSource{
		Config: config,
		Rows: &Rows{
			Columns: columns,
			Values:  values,
		},
	}
	return ds, err
}

// GetRows :
func (ds *CSVDataSource) GetRows() *Rows {
	return ds.Rows
}

// SetRows :
func (ds *CSVDataSource) SetRows(rows *Rows) {
	ds.Rows = rows
}

// Output :
func (ds *CSVDataSource) Output() error {
	file, err := os.Create(ds.Config.OutputPath)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, value := range append([][]string{ds.Rows.Columns}, ds.Rows.Values...) {
		err := writer.Write(value)
		if err != nil {
			return err
		}
	}
	return nil
}
