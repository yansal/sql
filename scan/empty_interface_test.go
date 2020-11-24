package scan

import (
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"
	"time"
)

func TestMap(t *testing.T) {
	now := time.Now()
	db := sql.OpenDB(&mockConnector{conn: &mockConn{stmt: &mockStmt{rows: &mockRows{
		columns: []string{"int", "string", "time", "null"},
		values: [][]driver.Value{
			{1, "hello", now, nil},
		},
	}}}})
	rows, err := db.Query("")
	if err != nil {
		t.Fatal(err)
	}

	m, err := Map(rows)
	if err != nil {
		t.Fatal(err)
	}
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
}

func TestMapErrNoRows(t *testing.T) {
	db := sql.OpenDB(&mockConnector{conn: &mockConn{stmt: &mockStmt{rows: &mockRows{
		columns: []string{"int", "string", "time", "null"},
	}}}})
	rows, err := db.Query("")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := Map(rows); errors.Is(err, sql.ErrNoRows) {
		// good path
	} else if err != nil {
		t.Fatal(err)
	} else {
		t.Error(`expected sql.ErrNoRows, got nil`)
	}
}

func TestMapSlice(t *testing.T) {
	now := time.Now()
	db := sql.OpenDB(&mockConnector{conn: &mockConn{stmt: &mockStmt{rows: &mockRows{
		columns: []string{"int", "string", "time", "null"},
		values: [][]driver.Value{
			{1, "hello", now, nil},
			{2, "world", now.Add(time.Second), nil},
		},
	}}}})
	rows, err := db.Query("")
	if err != nil {
		t.Fatal(err)
	}

	maps, err := MapSlice(rows)
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

func TestSliceSlice(t *testing.T) {
	now := time.Now()
	db := sql.OpenDB(&mockConnector{conn: &mockConn{stmt: &mockStmt{rows: &mockRows{
		columns: []string{"int", "string", "time", "null"},
		values: [][]driver.Value{
			{1, "hello", now, nil},
			{2, "world", now.Add(time.Second), nil},
		},
	}}}})
	rows, err := db.Query("")
	if err != nil {
		t.Fatal(err)
	}

	slices, err := SliceSlice(rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(slices) != 2 {
		t.Errorf("expected 2 rows, got %d", len(slices))
	}
	s := slices[0]
	if len(s) != 4 {
		t.Errorf("expected 4 columns, got %d", len(s))
	}
	if s[0] != 1 {
		t.Errorf("expected 1, got %d", s[0])
	}
	if s[1] != "hello" {
		t.Errorf(`expected "hello", got %s`, s[1])
	}
	if s[2] != now {
		t.Errorf(`expected now, got %s`, s[2])
	}
	if s[3] != nil {
		t.Errorf(`expected nil, got %s`, s[3])
	}

	s = slices[1]
	if len(s) != 4 {
		t.Errorf("expected 4 columns, got %d", len(s))
	}
	if s[0] != 2 {
		t.Errorf("expected 2, got %d", s[0])
	}
	if s[1] != "world" {
		t.Errorf(`expected "world", got %s`, s[1])
	}
	if s[2] != now.Add(time.Second) {
		t.Errorf(`expected now, got %s`, s[2])
	}
	if s[3] != nil {
		t.Errorf(`expected nil, got %s`, s[3])
	}
}
