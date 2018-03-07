package main

import (
	"github.com/Mitu217/tamate/database"
	"github.com/Mitu217/tamate/spreadSheets"
)

func main() {
	schema := schema.LoadSchema("./resources/database/definitions/sample.json")
	spreadSheets.OutputCSV(schema)
}
