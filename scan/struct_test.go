package scan

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"
	"time"
)

type T struct {
	Int    int64       `scan:"int"`
	String string      `scan:"string"`
	Time   time.Time   `scan:"time"`
	Null   interface{} `scan:"null"`
}

func TestQueryStruct(t *testing.T) {
	ctx := context.Background()

	now := time.Now()
	db := sql.OpenDB(&mockConnector{conn: &mockConn{stmt: &mockStmt{rows: &mockRows{
		columns: []string{"int", "string", "time", "null"},
		values:  [][]driver.Value{{1, "hello", now, nil}},
	}}}})
	var dest T
	if err := QueryStruct(ctx, db, &dest, ""); err != nil {
		t.Fatal(err)
	}
	if dest.Int != 1 {
		t.Errorf("expected 1, got %d", dest.Int)
	}
	if dest.String != "hello" {
		t.Errorf(`expected "hello", got %s`, dest.String)
	}
	if dest.Time != now {
		t.Errorf(`expected now, got %s`, dest.Time)
	}
	if dest.Null != nil {
		t.Errorf(`expected nil, got %s`, dest.Null)
	}
}

func TestQueryStructSlice(t *testing.T) {
	ctx := context.Background()

	now := time.Now()
	db := sql.OpenDB(&mockConnector{conn: &mockConn{stmt: &mockStmt{rows: &mockRows{
		columns: []string{"int", "string", "time", "null"},
		values: [][]driver.Value{
			{1, "hello", now, nil},
			{2, "world", now.Add(time.Second), nil},
		},
	}}}})
	var dest []T
	if err := QueryStructSlice(ctx, db, &dest, ""); err != nil {
		t.Fatal(err)
	}
	if len(dest) != 2 {
		t.Errorf("expected two rows, got %d", len(dest))
	}
	if dest[0].Int != 1 {
		t.Errorf("expected 1, got %d", dest[0].Int)
	}
	if dest[0].String != "hello" {
		t.Errorf(`expected "hello", got %q`, dest[0].String)
	}
	if dest[0].Time != now {
		t.Errorf(`expected now, got %s`, dest[0].Time)
	}
	if dest[0].Null != nil {
		t.Errorf(`expected nil, got %s`, dest[0].Null)
	}
	if dest[1].Int != 2 {
		t.Errorf("expected 2, got %d", dest[1].Int)
	}
	if dest[1].String != "world" {
		t.Errorf(`expected "world", got %q`, dest[1].String)
	}
	if dest[1].Time != now.Add(time.Second) {
		t.Errorf(`expected now, got %s`, dest[1].Time)
	}
	if dest[1].Null != nil {
		t.Errorf(`expected nil, got %s`, dest[1].Null)
	}
}
