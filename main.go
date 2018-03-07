package main

import (
	"github.com/Mitu217/tamate/schema"
	"github.com/Mitu217/tamate/spreadSheets"
)

func main() {
	sc, err := schema.NewJsonFileSchema("./resources/schema/sample.json")
	if err != nil {
		panic(err)
	}
	spreadSheets.OutputCSV(sc)
}
