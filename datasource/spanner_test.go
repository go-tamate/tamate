package datasource

import (
	"context"
	"os"
	"testing"

	"cloud.google.com/go/spanner"
	"cloud.google.com/go/spanner/admin/database/apiv1"
	"fmt"
	"github.com/google/uuid"
	adminpb "google.golang.org/genproto/googleapis/spanner/admin/database/v1"
	"time"
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
	ArrayTest            []int64
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
  ArrayTest ARRAY<INT64>,
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
			ArrayTest:            []int64{123, 456, 789},
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
	t.Logf("PK: %+v", sc.PrimaryKey)

	rows, err := ds.GetRows(ctx, sc)
	if err != nil {
		t.Fatal(err)
	}

	actualRowCount := 0
	for i, row := range rows.Values {
		if i == 0 {
			for key, val := range row {
				t.Logf("%+v: %+v", key, val)
			}
		}
		if _, err := uuid.Parse(row[0]); err != nil {
			t.Fatalf("invalid uuid: %s.", row[0])
		}
		if row[1] != fmt.Sprintf("testString%d", i) {
			t.Fatalf("TestString must be %s, but actual: %s .", fmt.Sprintf("testString%d", i), row[1])
		}
		// TODO: generic column value
		/*
			if row[2] != {
				t.Fatalf("alwaysNullString must be nil, but %+v found", row[2])
			}
		*/
		if row[3] != "123456" {
			t.Fatalf("IntTest value must be 123456, but actual: %s.", row[3])
		}
		// TODO: generic column value
		/*
			if row[4] != "123456.789" {
				t.Fatalf("FloatTest value must be 123456.789, but actual: %s", row[4])
			}
		*/
		if _, err := time.Parse(time.RFC3339Nano, row[5]); err != nil {
			t.Fatalf("TimestampTest must be '%s' format (actual: %s).", time.RFC3339Nano, row[5])
		}
		if _, err := time.Parse("2006-01-02", row[6]); err != nil {
			t.Fatalf("DateTest must be '2006-01-02' format (actual: %s).", row[6])
		}
		// TODO: generic column value
		/*
			if row[7] != "false" {
				t.Fatalf("BoolTest must be 'false', but actual: %s.", row[7])
			}
			expectedBase64 := base64.StdEncoding.EncodeToString([]byte(fmt.Sprintf("testBytes%d", i)))
			if row[7] != expectedBase64 {
				t.Fatalf("BytesTest must be %s, but actual: %s.", expectedBase64, row[7])
			}
		*/

		actualRowCount += 1
	}

	if actualRowCount != spannerTestDataRowCount {
		t.Fatalf("row count must be %d, but actual: %d", spannerTestDataRowCount, actualRowCount)
	}
}
