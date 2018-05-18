package datasource

import "testing"

func TestMockDatasource_GetRows(t *testing.T) {
	ds, err := NewMockDatasource()
	if err != nil {
		t.Fatal(err)
	}

	sc := &Schema{}
	if err := ds.GetSchema(sc); err != nil {
		t.Fatal(err)
	}

	rows, err := ds.GetRows(sc)
	if err != nil {
		t.Fatal(err)
	}

	for _, row := range rows.Values {
		t.Logf("%+v", row)
	}
}
