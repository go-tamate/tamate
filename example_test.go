package tamate_test

import (
	"context"
	"log"

	"github.com/go-tamate/tamate"
	"github.com/go-tamate/tamate/driver"
)

var (
	ctx context.Context
	ds  *tamate.DataSource
)

func ExampleRows() {
	rows, err := ds.GetRows(ctx, "name")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	nvss := make([][]*driver.NamedValue, 0)
	for rows.Next() {
		var nvs []*driver.NamedValue
		if err := rows.Scan(nvs); err != nil {
			log.Fatal(err)
		}
		nvss = append(nvss, nvs)
	}
	if err := rows.Err(); err != nil {
		log.Fatal(err)
	}
	log.Println(nvss)
}
