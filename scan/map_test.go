package scan

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"
	"time"
)

func TestQueryMap(t *testing.T) {
	ctx := context.Background()

	now := time.Now()
	db := sql.OpenDB(&mockConnector{conn: &mockConn{stmt: &mockStmt{rows: &mockRows{
		columns: []string{"int", "string", "time", "null"},
		values: [][]driver.Value{
			{1, "hello", now, nil},
			{2, "world", now.Add(time.Second), nil},
		},
	}}}})
	maps, err := QueryMapSlice(ctx, db, "")
	if err != nil {
		t.Fatal(err)
	}
	if len(maps) != 2 {
		t.Errorf("expected 2 rows, got %d", len(maps))
	}
	m := maps[0]
	if len(m) != 4 {
		t.Errorf("expected 4 columns, got %d", len(m))
	}
	if m["int"] != 1 {
		t.Errorf("expected 1, got %d", m["int"])
	}
	if m["string"] != "hello" {
		t.Errorf(`expected "hello", got %s`, m["string"])
	}
	if m["time"] != now {
		t.Errorf(`expected now, got %s`, m["time"])
	}
	if m["null"] != nil {
		t.Errorf(`expected nil, got %s`, m["null"])
	}

	m = maps[1]
	if len(m) != 4 {
		t.Errorf("expected 4 columns, got %d", len(m))
	}
	if m["int"] != 2 {
		t.Errorf("expected 2, got %d", m["int"])
	}
	if m["string"] != "world" {
		t.Errorf(`expected "world", got %s`, m["string"])
	}
	if m["time"] != now.Add(time.Second) {
		t.Errorf(`expected now, got %s`, m["time"])
	}
	if m["null"] != nil {
		t.Errorf(`expected nil, got %s`, m["null"])
	}
}
