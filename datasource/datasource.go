package datasource

import (
	"encoding/csv"
	"os"

	"github.com/Mitu217/tamate/schema"
)

type DataSource interface {
	GetColumns() ([]string, error)
	SetColumns([]string) error
	GetValues() ([][]string, error)
	SetValues([][]string) error
	OutputCSV(schema.Schema, string) error
}

func Output(path string, data [][]string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, value := range data {
		err := writer.Write(value)
		if err != nil {
			return err
		}
	}
	return nil
}
