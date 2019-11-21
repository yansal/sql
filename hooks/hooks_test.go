package hooks

import (
	"context"
	"database/sql/driver"
	"testing"
)

type mockConnector struct{}

func (c *mockConnector) Connect(context.Context) (driver.Conn, error) { return &mockConn{}, nil }
func (c *mockConnector) Driver() driver.Driver                        { return nil }

func TestConnectHook(t *testing.T) {
	var ok bool
	connector := &Connector{
		Wrapped:     &mockConnector{},
		ConnectHook: func(ctx context.Context) (driver.Conn, error) { ok = true; return nil, nil },
	}

	ctx := context.Background()
	if _, err := connector.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("expected ConnectHook to be called")
	}
}

type mockConn struct{}

func (c *mockConn) Begin() (driver.Tx, error)           { return nil, nil }
func (c *mockConn) Close() error                        { return nil }
func (c *mockConn) Prepare(string) (driver.Stmt, error) { return nil, nil }

func TestQueryHook(t *testing.T) {
	var ok bool
	var conn driver.Conn = &Conn{
		Wrapped: &mockConn{},
		QueryHook: func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
			ok = true
			return nil, nil
		},
	}

	ctx := context.Background()
	if _, err := conn.(driver.QueryerContext).QueryContext(ctx, "", nil); err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("expected QueryHook to be called")
	}
}

func TestConnectHookQueryHook(t *testing.T) {
	var ok bool
	connector := &Connector{Wrapped: &mockConnector{}}
	connector.ConnectHook = func(ctx context.Context) (driver.Conn, error) {
		conn, err := connector.Wrapped.Connect(ctx)
		return &Conn{
			Wrapped: conn,
			QueryHook: func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
				ok = true
				return nil, nil
			},
		}, err
	}

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
