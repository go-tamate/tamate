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
	Config  *CSVConfig
	Columns []string
	Values  [][]string
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
		Config:  config,
		Columns: columns,
		Values:  values,
	}
	return ds, err
}

// GetColumns :
func (ds *CSVDataSource) GetColumns() []string {
	return ds.Columns
}

// SetColumns :
func (ds *CSVDataSource) SetColumns(columns []string) {
	ds.Columns = columns
}

// GetValues :
func (ds *CSVDataSource) GetValues() [][]string {
	return ds.Values
}

// SetValues :
func (ds *CSVDataSource) SetValues(values [][]string) {
	ds.Values = values
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

	for _, value := range append([][]string{ds.Columns}, ds.Values...) {
		err := writer.Write(value)
		if err != nil {
			return err
		}
	}
	return nil
}
