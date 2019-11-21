package scan

import (
	"context"
	"database/sql/driver"
	"fmt"
	"io"
)

type mockConnector struct{ conn driver.Conn }

func (c *mockConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (c *mockConnector) Driver() driver.Driver                        { return nil }

type mockConn struct{ stmt driver.Stmt }

func (c *mockConn) Begin() (driver.Tx, error)           { return nil, nil }
func (c *mockConn) Close() error                        { return nil }
func (c *mockConn) Prepare(string) (driver.Stmt, error) { return c.stmt, nil }

type mockStmt struct{ rows driver.Rows }

func (s *mockStmt) Close() error                               { return nil }
func (s *mockStmt) Exec([]driver.Value) (driver.Result, error) { return nil, nil }
func (s *mockStmt) NumInput() int                              { return 0 }
func (s *mockStmt) Query([]driver.Value) (driver.Rows, error)  { return s.rows, nil }

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
		panic(fmt.Sprintf("expected %d values, got %d", len(r.values[0]), len(values)))
	}
	for i, v := range r.values[0] {
		values[i] = v
	}
	r.values = r.values[1:]
	return nil
}
