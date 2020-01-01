package load

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"testing"
)

type mockConnector struct{ conn driver.Conn }

func (c *mockConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (c *mockConnector) Driver() driver.Driver                        { return nil }

type mockConn struct {
	stmt  driver.Stmt
	query string
}

func (c *mockConn) Begin() (driver.Tx, error) { return nil, nil }
func (c *mockConn) Close() error              { return nil }
func (c *mockConn) Prepare(query string) (driver.Stmt, error) {
	if c.query != query {
		return nil, fmt.Errorf("expected %q, got %q", c.query, query)
	}
	return c.stmt, nil
}

type mockStmt struct {
	rows   driver.Rows
	values []driver.Value
}

func (s *mockStmt) Close() error                               { return nil }
func (s *mockStmt) Exec([]driver.Value) (driver.Result, error) { return nil, nil }
func (s *mockStmt) NumInput() int                              { return -1 }
func (s *mockStmt) Query(values []driver.Value) (driver.Rows, error) {
	if len(s.values) != len(values) {
		return nil, fmt.Errorf("expected %d values, got %d", len(s.values), len(values))
	}
	for i := range s.values {
		if s.values[i] != values[i] {
			return nil, fmt.Errorf("expected %v (%T), got %v (%T)", s.values[i], s.values[i], values[i], values[i])
		}
	}
	return s.rows, nil
}

type mockRows struct {
	columns []string
	values  [][]driver.Value
}

func (r *mockRows) Close() error      { return nil }
func (r *mockRows) Columns() []string { return r.columns }
func (r *mockRows) Next(values []driver.Value) error {
	if len(r.values) == 0 {
		return io.EOF
	}
	if len(r.values[0]) != len(values) {
		return fmt.Errorf("expected %d values, got %d", len(r.values[0]), len(values))
	}
	for i, v := range r.values[0] {
		values[i] = v
	}
	r.values = r.values[1:]
	return nil
}

type User struct {
	ID int64 `scan:"id"`

	Posts []Post `load:"posts_table.user_id = id"`
}

type Post struct {
	ID     int64 `scan:"id"`
	UserID int64 `scan:"user_id"`

	User *User `load:"users_table.id = user_id"`
}

func TestPtrField(t *testing.T) {
	var (
		ctx = context.Background()
		db  = sql.OpenDB(&mockConnector{conn: &mockConn{
			query: `SELECT "id" FROM "users_table" WHERE "id" = $1`,
			stmt: &mockStmt{
				values: []driver.Value{int64(456)},
				rows: &mockRows{
					columns: []string{"id"},
					values:  [][]driver.Value{{456}},
				}}}})
		p = &Post{ID: 123, UserID: 456}
	)
	if err := Struct(ctx, db, p, "User"); err != nil {
		t.Fatal(err)
	}
	if p.User == nil {
		t.Error("expected p.User to not be nil")
	}
	if p.User.ID != p.UserID {
		t.Errorf("expected p.User.ID to equal %d, got %d", p.UserID, p.User.ID)
	}
}

func TestSliceField(t *testing.T) {
	var (
		ctx = context.Background()
		db  = sql.OpenDB(&mockConnector{conn: &mockConn{
			query: `SELECT "id", "user_id" FROM "posts_table" WHERE "user_id" = $1`,
			stmt: &mockStmt{
				values: []driver.Value{int64(123)},
				rows: &mockRows{
					columns: []string{"id", "user_id"},
					values:  [][]driver.Value{{123, 456}, {123, 789}},
				}}}})
		u = &User{ID: 123}
	)
	if err := Struct(ctx, db, u, "Posts"); err != nil {
		t.Fatal(err)
	}
	if len(u.Posts) != 2 {
		t.Errorf("expected len(u.Posts) to equal %d, got %d", 2, len(u.Posts))
	}
}
