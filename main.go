package main

import (
	"github.com/Mitu217/tamate/datasource"
	"github.com/Mitu217/tamate/spreadSheets"
)

func main() {
	// Output mock
	/*
		sc, err := schema.NewJsonFileSchema("./resources/schema/sample.json")
		if err != nil {
			panic(err)
		}
		spreadSheets.OutputCSV(sc)
	*/

	// Input mock
	ds, err := datasource.NewCSVFileDataSource("./resources/datasource/csv/sample.csv")
	if err != nil {
		panic(err)
	}
	spreadSheets.SetSampleValues(ds.Values)
}
