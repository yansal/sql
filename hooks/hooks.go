package hooks

import (
	"context"
	"database/sql/driver"
)

type Connector struct {
	ConnectHook func(context.Context) (driver.Conn, error)
	Wrapped     driver.Connector
}

func (c *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	if c.ConnectHook != nil {
		return c.ConnectHook(ctx)
	}
	return c.Wrapped.Connect(ctx)
}

func (c *Connector) Driver() driver.Driver { return c.Wrapped.Driver() }

type Conn struct {
	ExecHook  func(ctx context.Context, query string, values []driver.NamedValue) (driver.Result, error)
	QueryHook func(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error)
	Wrapped   driver.Conn
}

func (c *Conn) Begin() (driver.Tx, error) {
	return c.Wrapped.Begin()
}

func (c *Conn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	return c.Wrapped.(driver.ConnBeginTx).BeginTx(ctx, opts)
}

func (c *Conn) Close() error {
	return c.Wrapped.Close()
}

func (c *Conn) Prepare(s string) (driver.Stmt, error) {
	return c.Wrapped.Prepare(s)
}

func (c *Conn) Ping(ctx context.Context) error {
	return c.Wrapped.(driver.Pinger).Ping(ctx)
}

func (c *Conn) Exec(query string, values []driver.Value) (driver.Result, error) {
	return c.Wrapped.(driver.Execer).Exec(query, values)
}

func (c *Conn) ExecContext(ctx context.Context, query string, values []driver.NamedValue) (driver.Result, error) {
	if c.ExecHook != nil {
		return c.ExecHook(ctx, query, values)
	}
	return c.Wrapped.(driver.ExecerContext).ExecContext(ctx, query, values)
}

func (c *Conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	return c.Wrapped.(driver.Queryer).Query(query, args)
}

func (c *Conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if c.QueryHook != nil {
		return c.QueryHook(ctx, query, args)
	}
	return c.Wrapped.(driver.QueryerContext).QueryContext(ctx, query, args)
}

var (
	_ driver.ConnBeginTx    = &Conn{}
	_ driver.Pinger         = &Conn{}
	_ driver.Execer         = &Conn{}
	_ driver.ExecerContext  = &Conn{}
	_ driver.Queryer        = &Conn{}
	_ driver.QueryerContext = &Conn{}
)
