package hooks

import (
	"context"
	"database/sql/driver"
	"testing"
)

type mockConnector struct{}

func (c *mockConnector) Connect(context.Context) (driver.Conn, error) { return &mockConn{}, nil }
func (c *mockConnector) Driver() driver.Driver                        { return nil }

type mockConn struct{}

func (c *mockConn) Begin() (driver.Tx, error)           { return &mockTx{}, nil }
func (c *mockConn) Close() error                        { return nil }
func (c *mockConn) Prepare(string) (driver.Stmt, error) { return &mockStmt{}, nil }

func (c *mockConn) Exec(string, []driver.Value) (driver.Result, error) { return nil, nil }
func (c *mockConn) Query(string, []driver.Value) (driver.Rows, error)  { return nil, nil }

func (c *mockConn) ExecContext(context.Context, string, []driver.NamedValue) (driver.Result, error) {
	return nil, nil
}
func (c *mockConn) QueryContext(context.Context, string, []driver.NamedValue) (driver.Rows, error) {
	return nil, nil
}

type mockTx struct{}

func (tx *mockTx) Commit() error   { return nil }
func (tx *mockTx) Rollback() error { return nil }

type mockStmt struct{}

func (s *mockStmt) Close() error                               { return nil }
func (s *mockStmt) Exec([]driver.Value) (driver.Result, error) { return nil, nil }
func (s *mockStmt) NumInput() int                              { return 0 }
func (s *mockStmt) Query([]driver.Value) (driver.Rows, error)  { return nil, nil }

func (s *mockStmt) ExecContext(context.Context, []driver.NamedValue) (driver.Result, error) {
	return nil, nil
}
func (s *mockStmt) QueryContext(context.Context, []driver.NamedValue) (driver.Rows, error) {
	return nil, nil
}

func TestExecHook(t *testing.T) {
	connector := Wrap(&mockConnector{})
	var ok bool
	connector.ExecHook = func(context.Context, ExecInfo) { ok = true }

	ctx := context.Background()
	conn, err := connector.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := conn.Prepare("")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := stmt.Exec(nil); err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Error("expected ExecHook to be called")
	}
}

func TestQueryHook(t *testing.T) {
	connector := Wrap(&mockConnector{})
	var ok bool
	connector.QueryHook = func(context.Context, QueryInfo) { ok = true }

	ctx := context.Background()
	conn, err := connector.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := conn.Prepare("")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := stmt.Query(nil); err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Error("expected QueryHook to be called")
	}
}

func TestStmtExecContextHook(t *testing.T) {
	connector := Wrap(&mockConnector{})
	var ok bool
	connector.ExecHook = func(context.Context, ExecInfo) { ok = true }

	ctx := context.Background()
	conn, err := connector.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := conn.Prepare("")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := stmt.(driver.StmtExecContext).ExecContext(ctx, nil); err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Error("expected ExecHook to be called")
	}
}

func TestStmtQueryContextHook(t *testing.T) {
	connector := Wrap(&mockConnector{})
	var ok bool
	connector.QueryHook = func(context.Context, QueryInfo) { ok = true }

	ctx := context.Background()
	conn, err := connector.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	stmt, err := conn.Prepare("")
	if err != nil {
		t.Fatal(err)
	}

	if _, err := stmt.(driver.StmtQueryContext).QueryContext(ctx, nil); err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Error("expected QueryHook to be called")
	}
}

func TestExecerHook(t *testing.T) {
	connector := Wrap(&mockConnector{})
	var ok bool
	connector.ExecHook = func(context.Context, ExecInfo) { ok = true }

	ctx := context.Background()
	conn, err := connector.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := conn.(driver.Execer).Exec("", nil); err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Error("expected ExecHook to be called")
	}
}

func TestQueryerHook(t *testing.T) {
	connector := Wrap(&mockConnector{})
	var ok bool
	connector.QueryHook = func(context.Context, QueryInfo) { ok = true }

	ctx := context.Background()
	conn, err := connector.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.(driver.Queryer).Query("", nil); err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Error("expected QueryHook to be called")
	}
}

func TestExecerContextHook(t *testing.T) {
	connector := Wrap(&mockConnector{})
	var ok bool
	connector.ExecHook = func(context.Context, ExecInfo) { ok = true }

	ctx := context.Background()
	conn, err := connector.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := conn.(driver.ExecerContext).ExecContext(ctx, "", nil); err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Error("expected ExecHook to be called")
	}
}

func TestQueryerContextHook(t *testing.T) {
	connector := Wrap(&mockConnector{})
	var ok bool
	connector.QueryHook = func(context.Context, QueryInfo) { ok = true }

	ctx := context.Background()
	conn, err := connector.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.(driver.QueryerContext).QueryContext(ctx, "", nil); err != nil {
		t.Fatal(err)
	}

	if !ok {
		t.Error("expected QueryHook to be called")
	}
}

func TestBeginCommitRollbackHook(t *testing.T) {
	connector := Wrap(&mockConnector{})
	var begin, commit, rollback bool
	connector.BeginHook = func(context.Context, BeginInfo) { begin = true }
	connector.CommitHook = func(CommitInfo) { commit = true }
	connector.RollbackHook = func(RollbackInfo) { rollback = true }

	ctx := context.Background()
	conn, err := connector.Connect(ctx)
	if err != nil {
		t.Fatal(err)
	}
	tx, err := conn.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if !begin {
		t.Error("expected BeginHook to be called")
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	if !commit {
		t.Error("expected CommitHook to be called")
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if !rollback {
		t.Error("expected RollbackHook to be called")
	}
}
