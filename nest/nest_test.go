package nest

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"regexp"
	"testing"

	"github.com/yansal/sql/hooks"
)

type mockConnector struct{ conn driver.Conn }

func (c *mockConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (c *mockConnector) Driver() driver.Driver                        { return nil }

type mockConn struct{ tx driver.Tx }

func (c *mockConn) Begin() (driver.Tx, error)           { return c.tx, nil }
func (c *mockConn) Close() error                        { return nil }
func (c *mockConn) Prepare(string) (driver.Stmt, error) { return nil, nil }

func (c *mockConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return nil, nil
}

type mockTx struct{}

func (tx *mockTx) Commit() error   { return nil }
func (tx *mockTx) Rollback() error { return nil }

var (
	insertRegexp    = regexp.MustCompile(`^INSERT INTO table1 VALUES \(\d+\)$`)
	savepointRegexp = regexp.MustCompile(`^SAVEPOINT \w+$`)
	rollbackRegexp  = regexp.MustCompile(`^ROLLBACK TO SAVEPOINT \w+$`)
	releaseRegexp   = regexp.MustCompile(`^RELEASE SAVEPOINT \w+$`)
)

func TestRollback(t *testing.T) {
	var (
		begins  []hooks.BeginInfo
		commits []hooks.CommitInfo
		execs   []hooks.ExecInfo
	)
	conn := hooks.Wrap(&mockConnector{conn: &mockConn{tx: &mockTx{}}})
	conn.BeginHook = func(ctx context.Context, info hooks.BeginInfo) { begins = append(begins, info) }
	conn.CommitHook = func(info hooks.CommitInfo) { commits = append(commits, info) }
	conn.ExecHook = func(ctx context.Context, info hooks.ExecInfo) { execs = append(execs, info) }
	db := Wrap(sql.OpenDB(conn))

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO table1 VALUES (1)"); err != nil {
		t.Fatal(err)
	}
	nesttx, err := tx.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nesttx.ExecContext(ctx, "INSERT INTO table1 VALUES (2)"); err != nil {
		t.Fatal(err)
	}
	if err := nesttx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO table1 VALUES (3)"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if len(begins) != 1 {
		t.Errorf("expected 1 begin, got %d", len(begins))
	}
	if len(commits) != 1 {
		t.Errorf("expected 1 commit, got %d", len(commits))
	}
	if len(execs) != 5 {
		t.Errorf("expected 5 execs, got %d", len(execs))
	}
	if !insertRegexp.MatchString(execs[0].Query) {
		t.Errorf("expected insert, got %q", execs[0].Query)
	}
	if !savepointRegexp.MatchString(execs[1].Query) {
		t.Errorf("expected savepoint, got %q", execs[1].Query)
	}
	if !insertRegexp.MatchString(execs[2].Query) {
		t.Errorf("expected insert, got %q", execs[2].Query)
	}
	if !rollbackRegexp.MatchString(execs[3].Query) {
		t.Errorf("expected rollback, got %q", execs[3].Query)
	}
	if !insertRegexp.MatchString(execs[4].Query) {
		t.Errorf("expected insert, got %q", execs[4].Query)
	}
}

func TestRelease(t *testing.T) {
	var (
		begins  []hooks.BeginInfo
		commits []hooks.CommitInfo
		execs   []hooks.ExecInfo
	)
	conn := hooks.Wrap(&mockConnector{conn: &mockConn{tx: &mockTx{}}})
	conn.BeginHook = func(ctx context.Context, info hooks.BeginInfo) { begins = append(begins, info) }
	conn.CommitHook = func(info hooks.CommitInfo) { commits = append(commits, info) }
	conn.ExecHook = func(ctx context.Context, info hooks.ExecInfo) { execs = append(execs, info) }
	db := Wrap(sql.OpenDB(conn))

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, "INSERT INTO table1 VALUES (3)"); err != nil {
		t.Fatal(err)
	}
	nesttx, err := tx.BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := nesttx.ExecContext(ctx, "INSERT INTO table1 VALUES (4)"); err != nil {
		t.Fatal(err)
	}
	if err := nesttx.Commit(); err != nil {
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	if len(begins) != 1 {
		t.Errorf("expected 1 begin, got %d", len(begins))
	}
	if len(commits) != 1 {
		t.Errorf("expected 1 commit, got %d", len(commits))
	}
	if len(execs) != 4 {
		t.Errorf("expected 4 execs, got %d", len(execs))
	}
	if !insertRegexp.MatchString(execs[0].Query) {
		t.Errorf("expected insert, got %q", execs[0].Query)
	}
	if !savepointRegexp.MatchString(execs[1].Query) {
		t.Errorf("expected savepoint, got %q", execs[1].Query)
	}
	if !insertRegexp.MatchString(execs[2].Query) {
		t.Errorf("expected insert, got %q", execs[2].Query)
	}
	if !releaseRegexp.MatchString(execs[3].Query) {
		t.Errorf("expected release, got %q", execs[3].Query)
	}
}
