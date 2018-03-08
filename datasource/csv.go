package datasource

import (
	"encoding/csv"
	"io"
	"os"
)

func NewCSVDataSource(r io.Reader) (*CSVDataSource, error) {
	recodes, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, err
	}
	values := make([][]interface{}, len(recodes))
	for i, recode := range recodes {
		value := make([]interface{}, len(recode))
		for k, v := range recode {
			value[k] = v
		}
		values[i] = value
	}
	ds := &CSVDataSource{
		Values: values,
	}
	return ds, err
}

func NewCSVFileDataSource(path string) (*CSVDataSource, error) {
	r, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return NewCSVDataSource(r)
}

/*
func OutputCSV(schema *schema.Schema, columns []string, rows [][]string) error {
	var data [][]string
	for _, row := range rows {
		var datum []string
		for _, property := range schema.Properties {
			index := contains(columns, property.Name)
			if index == -1 {
				// set default value.
				if property.NotNull {
					// typeに応じて綺麗に対応する方法を考える（デフォルト値対応も）
					if property.Type == "datetime" {
						datum = append(datum, time.Now().Format("2006-01-02 15:04:05"))
					} else {
						datum = append(datum, "")
					}
				} else {
					datum = append(datum, "")
				}
			} else {
				datum = append(datum, row[index])
			}
		}
		data = append(data, datum)
	}

	file, err := os.Create("sample.csv")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	for _, value := range data {
		err := writer.Write(value)
		if err != nil {
			panic(err)
		}
	}

	return nil
}
*/

func (ds *CSVDataSource) OutputCSV(path string) error {
	return nil
}

func contains(s []string, e string) int {
	for i, v := range s {
		if e == v {
			return i
		}
	}
	return -1
}

// CSV data source
type CSVDataSource struct {
	Values [][]interface{}
}
