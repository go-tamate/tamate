package main

import (
	"tamate/database"
	"tamate/spreadSheets"
)

func main() {
	schema := schema.LoadSchema("./resources/database/definitions/sample.json")
	spreadSheets.OutputCSV(schema)
}
