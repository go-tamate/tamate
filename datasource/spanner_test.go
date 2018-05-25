package datasource

import (
	"context"
	"os"
	"testing"

	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1"
	"github.com/google/uuid"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
)

const (
	spannerTestDatabaseID   = "tamatest"
	spannerTestTableName    = "Test"
	spannerTestDataRowCount = 1
)

type testStruct struct {
	ID                   string
	StringTest           string
	AlwaysNullStringTest spanner.NullString
	IntTest              int64
	FloatTest            float64
	TimestampTest        time.Time
	DateTest             string
	BoolTest             bool
	BytesTest            []byte
	Int64ArrayTest       []int64
	Float64ArrayTest     []float64
	StringArrayTest      []string
	BytesArrayTest       [][]byte
	DateArrayTest        []string
	TimestampArrayTest   []time.Time
	BoolArrayTest        []bool
}

func beforeSpanner(dsnParent string) error {
	ctx := context.Background()
	c, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}

	req := &adminpb.CreateDatabaseRequest{
		Parent:          dsnParent,
		CreateStatement: "CREATE DATABASE " + spannerTestDatabaseID,
		ExtraStatements: []string{fmt.Sprintf(`
CREATE TABLE %s (
  ID STRING(MAX) NOT NULL,
  StringTest STRING(MAX),
  AlwaysNullStringTest STRING(MAX),
  IntTest INT64,
  FloatTest FLOAT64,
  TimestampTest TIMESTAMP,
  DateTest DATE,
  BoolTest BOOL,
  BytesTest BYTES(MAX),
  Int64ArrayTest ARRAY<INT64>,
  Float64ArrayTest ARRAY<FLOAT64>,
  StringArrayTest ARRAY<STRING(MAX)>,
  BytesArrayTest ARRAY<BYTES(MAX)>,
  DateArrayTest ARRAY<DATE>,
  TimestampArrayTest ARRAY<TIMESTAMP>,
  BoolArrayTest ARRAY<BOOL>,
) PRIMARY KEY(ID)
`, spannerTestTableName)},
	}

	op, err := c.CreateDatabase(ctx, req)
	if err != nil {
		return err
	}

	if _, err := op.Wait(ctx); err != nil {
		return err
	}

	sc, err := spanner.NewClient(ctx, fmt.Sprintf("%s/databases/%s", dsnParent, spannerTestDatabaseID))
	if err != nil {
		return err
	}

	var ms []*spanner.Mutation
	for i := 0; i < spannerTestDataRowCount; i++ {
		id, err := uuid.NewRandom()
		if err != nil {
			return err
		}
		//id2, err := uuid.NewRandom()
		//if err != nil {
		//	return err
		//}
		//id3, err := uuid.NewRandom()
		//if err != nil {
		//	return err
		//}
		ts := &testStruct{
			ID:                   id.String(),
			StringTest:           fmt.Sprintf("testString%d", i),
			AlwaysNullStringTest: spanner.NullString{Valid: false},
			IntTest:              123456,
			FloatTest:            123456.789,
			TimestampTest:        time.Now(),
			DateTest:             time.Now().Format("2006-01-02"),
			BoolTest:             true,
			BytesTest:            []byte(fmt.Sprintf("testBytes%d", i)),
			Int64ArrayTest:       []int64{123, 456, -789},
			Float64ArrayTest:     []float64{3.2, 1.4, 4.3, -2.2, 0.8},
			StringArrayTest:      []string{"foo", "bar", "hoge"},
			BytesArrayTest:       [][]byte{[]byte(fmt.Sprintf("bytesArray%d", i)), []byte(fmt.Sprintf("bytesArray%d", i*i)), []byte(fmt.Sprintf("bytesArray%d", i*i*i))},
			DateArrayTest:        []string{time.Now().Format("2006-01-02"), time.Now().Add(60 * time.Minute).Format("2006-01-02"), time.Now().Add(120 * time.Minute).Format("2006-01-02")},
			TimestampArrayTest:   []time.Time{time.Now(), time.Now().Add(2 * time.Hour), time.Now().Add(24 * time.Hour)},
			BoolArrayTest:        []bool{true, false, false, false, true},
		}
		//ts2 := &testStruct{
		//	ID:                   id2.String(),
		//	StringTest:           fmt.Sprintf("testString%d", i),
		//	AlwaysNullStringTest: spanner.NullString{Valid: false},
		//	IntTest:              123456,
		//	FloatTest:            123456.789,
		//	TimestampTest:        time.Now(),
		//	DateTest:             time.Now().Format("2006-01-02"),
		//	BoolTest:             true,
		//	BytesTest:            []byte(fmt.Sprintf("testBytes%d", i)),
		//	Int64ArrayTest:       []int64{},
		//	Float64ArrayTest:     []float64{},
		//	StringArrayTest:      []string{},
		//	BytesArrayTest:       [][]byte{},
		//	DateArrayTest:        []string{},
		//	TimestampArrayTest:   []time.Time{},
		//	BoolArrayTest:        []bool{},
		//}
		//ts3 := &testStruct{
		//	ID:                   id3.String(),
		//	StringTest:           fmt.Sprintf("testString%d", i),
		//	AlwaysNullStringTest: spanner.NullString{Valid: false},
		//	IntTest:              123456,
		//	FloatTest:            123456.789,
		//	TimestampTest:        time.Now(),
		//	DateTest:             time.Now().Format("2006-01-02"),
		//	BoolTest:             true,
		//	BytesTest:            []byte(fmt.Sprintf("testBytes%d", i)),
		//	Int64ArrayTest:       nil,
		//	Float64ArrayTest:     nil,
		//	StringArrayTest:      nil,
		//	BytesArrayTest:       nil,
		//	DateArrayTest:        nil,
		//	TimestampArrayTest:   nil,
		//	BoolArrayTest:        nil,
		//}
		m, err := spanner.InsertStruct(spannerTestTableName, ts)
		if err != nil {
			return err
		}
		//m2, err := spanner.InsertStruct(spannerTestTableName, ts2)
		//if err != nil {
		//	return err
		//}
		//m3, err := spanner.InsertStruct(spannerTestTableName, ts3)
		//if err != nil {
		//	return err
		//}
		//ms = append(ms, m, m2, m3)
		ms = append(ms, m)
	}
	if _, err := sc.Apply(ctx, ms); err != nil {
		return err
	}

	return nil
}

func afterSpanner(dsnParent string) error {
	ctx := context.Background()
	c, err := database.NewDatabaseAdminClient(ctx)
	if err != nil {
		return err
	}

	req := &adminpb.DropDatabaseRequest{
		Database: fmt.Sprintf("%s/databases/%s", dsnParent, spannerTestDatabaseID),
	}
	return c.DropDatabase(ctx, req)
}

func TestSpanner_Get(t *testing.T) {
	dsnParent := os.Getenv("TAMATE_SPANNER_DSN_PARENT")
	if dsnParent == "" {
		t.Skip("env: TAMATE_SPANNER_DSN_PARENT not set")
	}

	if err := beforeSpanner(dsnParent); err != nil {
		t.Fatal(err)
	}
	defer (func() {
		if err := afterSpanner(dsnParent); err != nil {
			t.Fatal(err)
		}
	})()

	dsn := fmt.Sprintf("%s/databases/%s", dsnParent, spannerTestDatabaseID)
	ds, err := NewSpannerDatasource(dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer ds.Close()

	ctx := context.Background()
	sc, err := ds.GetSchema(ctx, spannerTestTableName)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Schema: %+v", sc)

	rows, err := ds.GetRows(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}

	actualRowCount := 0
	for i, row := range rows {
		if i == 0 {
			for key, val := range row.Values {
				t.Logf("%+v: %+v", key, val)
			}
		}
		if _, err := uuid.Parse(row.Values["ID"].StringValue()); err != nil {
			t.Fatalf("invalid uuid: %s.", row.Values["ID"].StringValue())
		}
		if row.Values["StringTest"].StringValue() != fmt.Sprintf("testString%d", i) {
			t.Fatalf("StringTest must be %s, but actual: %s .", fmt.Sprintf("testString%d", i), row.Values["StringTest"].StringValue())
		}
		if row.Values["AlwaysNullStringTest"].Value != nil {
			t.Fatalf("AlwaysNullStringTest must be nil, but %+v found", row.Values["AlwaysNullStringTest"].Value)
		}
		if row.Values["IntTest"].Value != int64(123456) {
			t.Fatalf("IntTest value must be int64(123456), but actual: %+v.", row.Values["IntTest"].Value)
		}
		if row.Values["DateTest"].Value != time.Now().Format("2006-01-02") {
			t.Fatalf("DateTest value must be yyyy-mm-dd format(%s), but actual: %+v).", time.Now().Format("2006-01-02"), row.Values["DateTest"].Value)
		}
		if row.Values["DateTest"].Column.Type != ColumnTypeDate {
			t.Fatalf("DateTest ColumnType must be ColumnTypeDate(%d), but actual: %d.", ColumnTypeDate, row.Values["DateTest"].Column.Type)
		}
		// TODO: generic column value
		/*
				if row[4] != "123456.789" {
					t.Fatalf("FloatTest value must be 123456.789, but actual: %s", row[4])
				}
			if _, err := time.Parse(time.RFC3339Nano, row[5]); err != nil {
				t.Fatalf("TimestampTest must be '%s' format (actual: %s).", time.RFC3339Nano, row[5])
			}
			// TODO: generic column value
				if row[7] != "false" {
					t.Fatalf("BoolTest must be 'false', but actual: %s.", row[7])
				}
				expectedBase64 := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("testBytes%d", i)))
				if row[7] != expectedBase64 {
					t.Fatalf("BytesTest must be %s, but actual: %s.", expectedBase64, row[7])
				}
		*/

		actualRowCount++
	}

	if actualRowCount != spannerTestDataRowCount {
		t.Fatalf("row count must be %d, but actual: %d", spannerTestDataRowCount, actualRowCount)
	}
}
