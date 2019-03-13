package csv

import (
	"context"
	"log"
	"strings"
	"testing"

	"github.com/Mitu217/tamate/driver"

	"github.com/Mitu217/tamate"
	"github.com/stretchr/testify/assert"
)

func Test_Open(t *testing.T) {
	_, err := tamate.Open(driverName, "")
	assert.NoError(t, err)
}

func Test_GetSchema(t *testing.T) {
	var (
		rootDir  = "./"
		fileName = "getSchema"
		testData = `
			(id),name,age
		`
	)
	err := createCSVFile(rootDir, fileName, testData)
	assert.NoError(t, err)
	defer delete(rootDir, fileName)

	ds, err := tamate.Open(driverName, rootDir)
	if assert.NoError(t, err) {
		ctx := context.Background()
		schema, err := ds.DriverConn().GetSchema(ctx, fileName)
		if assert.NoError(t, err) {
			columns := schema.Columns
			assert.Equal(t, driver.ColumnTypeString, columns[0].Type)
			assert.Equal(t, "id", columns[0].Name)
			assert.Equal(t, 0, columns[0].OrdinalPosition)

			assert.Equal(t, driver.ColumnTypeString, columns[1].Type)
			assert.Equal(t, "name", columns[1].Name)
			assert.Equal(t, 1, columns[1].OrdinalPosition)

			assert.Equal(t, driver.ColumnTypeString, columns[2].Type)
			assert.Equal(t, "age", columns[2].Name)
			assert.Equal(t, 2, columns[2].OrdinalPosition)
		}
	}
}

func Test_SetSchema(t *testing.T) {
	var (
		rootDir    = "./"
		fileName   = "setSchema"
		beforeData = `
			(id),name,age
		`
		afterData = `
			(id),name,from
		`
	)
	err := createCSVFile(rootDir, fileName, beforeData)
	assert.NoError(t, err)
	defer delete(rootDir, fileName)

	log.Println(afterData)
}

func Test_GetRows(t *testing.T) {
	var (
		rootDir  = "./"
		fileName = "getRows"
		testData = `
			(id),name,age
			1,hana,16
		`
	)
	err := createCSVFile(rootDir, fileName, testData)
	assert.NoError(t, err)
	defer delete(rootDir, fileName)

}

func Test_SetRows(t *testing.T) {
	var (
		rootDir    = "./"
		fileName   = "setRows"
		beforeData = `
			(id),name,age
			1,hana,16
		`
		afterData = `
			(id),name,age
			1,tamate,15
		`
	)
	err := createCSVFile(rootDir, fileName, beforeData)
	assert.NoError(t, err)
	defer delete(rootDir, fileName)

	log.Println(afterData)
}

func createCSVFile(rootDir, fileName, data string) error {
	r := strings.NewReader(data)
	values, err := read(r)
	if err != nil {
		return err
	}
	return writeToFile(rootDir, fileName, values)
}
