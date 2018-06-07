package datasource

import (
	"context"
	"testing"

	"fmt"
	"time"

	"github.com/castaneai/spadmin"

	"cloud.google.com/go/spanner"
	"os"
	"reflect"
	"strings"
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

func spannerTestCase(t *testing.T, fun func(*SpannerDatasource) error) {
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
	stmts := []string{fmt.Sprintf(`
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
`, spannerTestTableName)}

	admin, err := spadmin.NewClient(dsnParent)
	if err != nil {
		return err
	}

	ctx := context.Background()
	if err := admin.CreateDatabase(ctx, spannerTestDatabaseID, stmts); err != nil {
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
	admin, err := spadmin.NewClient(dsnParent)
	if err != nil {
		return err
	}

	ctx := context.Background()
	return admin.DropDatabase(ctx, spannerTestDatabaseID)
}

func TestSpanner_Get(t *testing.T) {
	spannerTestCase(t, func(ds *SpannerDatasource) error {
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
			if !strings.HasPrefix(row.Values["ID"].StringValue(), "ID") {
				t.Fatalf("ID must have prefix: ID, but actual: %+v.", row.Values["ID"].Value)
			}
			if !strings.HasPrefix(row.Values["StringTest"].StringValue(), "testString") {
				t.Fatalf("StringTest must have prefix: testString, but actual: %+v.", row.Values["StringTest"].Value)
			}
			if row.Values["AlwaysNullStringTest"].Value != nil {
				t.Fatalf("AlwaysNullStringTest must be nil, but %+v found", row.Values["AlwaysNullStringTest"].Value)
			}
			if row.Values["IntTest"].Value != int64(123456) {
				t.Fatalf("IntTest value must be int64(123456), but actual: %+v.", row.Values["IntTest"].Value)
			}
			if _, err := time.Parse("2006-01-02", row.Values["DateTest"].StringValue()); err != nil {
				t.Fatalf("DateTest value must be yyyy-mm-dd format, but actual: %+v.", row.Values["DateTest"].Value)
			}
			if row.Values["DateTest"].Column.Type != ColumnTypeDate {
				t.Fatalf("DateTest ColumnType must be ColumnTypeDate(%d), but actual: %d.", ColumnTypeDate, row.Values["DateTest"].Column.Type)
			}
			if !reflect.DeepEqual(row.Values["Int64ArrayTest"].Value, []int64{123, 456, -789}) {
				t.Fatalf("Int64ArrayTest must be []int64{123, 456, -789}, but actual: %+v.", row.Values["Int64ArrayTest"].Value)
			}
			actualRowCount++
		}
		if actualRowCount != spannerTestDataRowCount {
			t.Fatalf("row count must be %d, but actual: %d", spannerTestDataRowCount, actualRowCount)
		}
		return nil
	})
}
