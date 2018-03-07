package main

import (
	"github.com/Mitu217/tamate/database"
	"github.com/Mitu217/tamate/datasource"
	"github.com/Mitu217/tamate/schema"
)

func main() {
	// Output mock
	/*
		sc, err := schema.NewJsonFileSchema("./resources/schema/sample.json")
		if err != nil {
			panic(err)
		}
		spreadsheets.OutputCSV(sc)
	*/

	// Input mock
	/*
		ds, err := datasource.NewCSVFileDataSource("./resources/datasource/csv/sample.csv")
		if err != nil {
			panic(err)
		}
		spreadsheets.SetSampleValues(ds.Values)
	*/

	// Dump sql mock
	sc, err := schema.NewJsonFileSchema("./resources/schema/sample.json")
	server, err := database.NewJsonFileServer("./resources/host/mysql/sample.json")
	if err != nil {
		panic(err)
	}
	sql := &database.SQLDatabase{
		Server: server,
		Name:   "Sample",
	}
	if err = sql.Dump(sc); err != nil {
		panic(err)
	}

	for _, table := range sql.Tables {
		datasource.OutputCSV(sc, table.Columns, table.Records)
	}
}
