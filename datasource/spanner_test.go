package datasource

import (
	"context"
	"testing"

	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"os"
)

const (
	spannerTestDatabaseID   = "tamatest"
	spannerTestTableName    = "Test"
	spannerTestDataRowCount = 100
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

func spannerTestCase(t *testing.T, fun func(*spannerDatasource) error) {
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

	if err := fun(ds); err != nil {
		t.Fatal(err)
	}
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
		ts := &testStruct{
			ID:                   fmt.Sprintf("ID%d", i),
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
		m, err := spanner.InsertStruct(spannerTestTableName, ts)
		if err != nil {
			return err
		}
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
	spannerTestCase(t, func(ds *spannerDatasource) error {
		ctx := context.Background()
		sc, err := ds.GetSchema(ctx, spannerTestTableName)
		if err != nil {
			return err
		}
		t.Logf("Schema: %+v", sc)

		rows, err := ds.GetRows(ctx, sc)
		if err != nil {
			return err
		}

		actualRowCount := 0
		for i, row := range rows {
			if i == 0 {
				for key, val := range row.Values {
					t.Logf("%+v: %+v", key, val)
				}
			}
			if row.Values["ID"].StringValue() != fmt.Sprintf("ID%d", i) {
				t.Fatalf("ID must be %s, but actual: %+v.", fmt.Sprintf("ID%d", i), row.Values["ID"].Value)
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
			actualRowCount++
		}
		if actualRowCount != spannerTestDataRowCount {
			t.Fatalf("row count must be %d, but actual: %d", spannerTestDataRowCount, actualRowCount)
		}
		return nil
	})
}
