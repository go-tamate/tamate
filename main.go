package main

import (
	"github.com/Mitu217/tamate/schema"
	"github.com/Mitu217/tamate/server"
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
	/**
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
	*/

	// Restore sql mock
	/*
		sc, err := schema.NewJsonFileSchema("./resources/schema/sample.json")
		ds, err := datasource.NewCSVFileDataSource("./resources/datasource/csv/sample.csv")
		if err != nil {
			panic(err)
		}
		server, err := database.NewJsonFileServer("./resources/host/mysql/sample.json")
		if err != nil {
			panic(err)
		}
		sql := &database.SQLDatabase{
			Server: server,
			Name:   "Sample",
		}
		if err = sql.Restore(sc, ds.Values); err != nil {
			panic(err)
		}
	*/

	// sql to schema
	server, err := server.NewJsonFileServer("./resources/host/mysql/sample.json")
	if err != nil {
		panic(err)
	}
	sc := &schema.SQLSchema{
		Server:       server,
		DatabaseName: "Sample",
	}
	sc.NewServerSchema("Sample")
	sc.Output("sample.json")
}
