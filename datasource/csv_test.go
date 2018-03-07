package datasource

import (
	"strings"
	"testing"
)

func TestNewJsonSchema(t *testing.T) {
	datasourceCSV := `
1,hana,16,2018-03-07 20:45:34
2,tamate,15,2018-03-07 20:45:34
3,eiko,15,2018-03-07 20:45:34
4,kamuri,15,2018-03-07 20:45:34
`

	r := strings.NewReader(datasourceCSV)
	_, err := NewCSVDataSource(r)
	if err != nil {
		t.Fatal(err)
	}
}
