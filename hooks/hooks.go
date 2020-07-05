package hooks

import (
	"context"
	"database/sql/driver"
	"time"
)

// Wrap returns a new Connector wrapping c.
func Wrap(c driver.Connector) *Connector { return &Connector{wrapped: c} }

// A Connector wraps an existing connector.
type Connector struct {
	ExecHook  func(ctx context.Context, info ExecInfo)
	QueryHook func(ctx context.Context, info QueryInfo)

	BeginHook    func(ctx context.Context, info BeginInfo)
	CommitHook   func(info CommitInfo)
	RollbackHook func(info RollbackInfo)

	// TODO: add ConnectHook, PrepareHook, etc.

	wrapped driver.Connector
}

// ExecInfo is the argument of ExecHook and contains information about the executed query.
type ExecInfo struct {
	Query    string
	Args     []driver.Value
	Duration time.Duration
	Err      error
}

// QueryInfo is the argument of QueryHook and contains information about the executed query.
type QueryInfo struct {
	Query    string
	Args     []driver.Value
	Duration time.Duration
	Err      error
}

// BeginInfo is the argument of BeginHook.
type BeginInfo struct {
	Duration time.Duration
	Err      error
}

// CommitInfo is the argument of CommitHook.
type CommitInfo struct {
	Duration time.Duration
	Err      error
}

// RollbackInfo is the argument of RollbackHook.
type RollbackInfo struct {
	Duration time.Duration
	Err      error
}

// Connect implements database/sql/driver.Connector.
func (connector *Connector) Connect(ctx context.Context) (driver.Conn, error) {
	c, err := connector.wrapped.Connect(ctx)
	if err != nil {
		return nil, err
	}
	return &conn{
		wrapped:      c,
		execHook:     connector.ExecHook,
		queryHook:    connector.QueryHook,
		beginHook:    connector.BeginHook,
		commitHook:   connector.CommitHook,
		rollbackHook: connector.RollbackHook,
	}, nil
}

// Driver implements database/sql/driver.Connector.
func (connector *Connector) Driver() driver.Driver { return connector.wrapped.Driver() }

type conn struct {
	wrapped      driver.Conn
	execHook     func(ctx context.Context, info ExecInfo)
	queryHook    func(ctx context.Context, info QueryInfo)
	beginHook    func(ctx context.Context, info BeginInfo)
	commitHook   func(info CommitInfo)
	rollbackHook func(info RollbackInfo)
}

func (c *conn) Begin() (driver.Tx, error) {
	start := time.Now()
	t, err := c.wrapped.Begin()
	if c.beginHook != nil {
		c.beginHook(context.Background(), BeginInfo{
			Duration: time.Since(start),
			Err:      err,
		})
	}
	return &tx{
		wrapped:      t,
		commitHook:   c.commitHook,
		rollbackHook: c.rollbackHook,
	}, nil
}

func (c *conn) Close() error {
	return c.wrapped.Close()
}

func (c *conn) Prepare(query string) (driver.Stmt, error) {
	s, err := c.wrapped.Prepare(query)
	if err != nil {
		return nil, err
	}
	return &stmt{
		wrapped:   s,
		query:     query,
		execHook:  c.execHook,
		queryHook: c.queryHook,
	}, nil
}

var (
	_ driver.Execer         = &conn{}
	_ driver.ExecerContext  = &conn{}
	_ driver.Queryer        = &conn{}
	_ driver.QueryerContext = &conn{}

	// _ driver.ConnBeginTx = &conn{}
	// _ driver.ConnPrepareContext = &conn{}
	// _ driver.NamedValueChecker      = &conn{}
	// _ driver.Pinger      = &conn{}
	// _ driver.SessionResetter = &conn{}
)

func (c *conn) Exec(query string, args []driver.Value) (driver.Result, error) {
	start := time.Now()
	res, err := c.wrapped.(driver.Execer).Exec(query, args)
	if c.execHook != nil {
		c.execHook(context.Background(), ExecInfo{
			Query:    query,
			Args:     args,
			Duration: time.Since(start),
			Err:      err,
		})
	}
	return res, err
}

func (c *conn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	start := time.Now()
	res, err := c.wrapped.(driver.ExecerContext).ExecContext(ctx, query, args)
	if c.execHook != nil {
		values := make([]driver.Value, 0, len(args))
		for i := range args {
			values = append(values, args[i].Value)
		}
		c.execHook(ctx, ExecInfo{
			Query:    query,
			Args:     values,
			Duration: time.Since(start),
			Err:      err,
		})
	}
	return res, err
}

func (c *conn) Query(query string, args []driver.Value) (driver.Rows, error) {
	start := time.Now()
	res, err := c.wrapped.(driver.Queryer).Query(query, args)
	if c.queryHook != nil {
		c.queryHook(context.Background(), QueryInfo{
			Query:    query,
			Args:     args,
			Duration: time.Since(start),
			Err:      err,
		})
	}
	return res, err
}

func (c *conn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	start := time.Now()
	rows, err := c.wrapped.(driver.QueryerContext).QueryContext(ctx, query, args)
	if c.queryHook != nil {
		values := make([]driver.Value, 0, len(args))
		for i := range args {
			values = append(values, args[i].Value)
		}
		c.queryHook(ctx, QueryInfo{
			Query:    query,
			Args:     values,
			Duration: time.Since(start),
			Err:      err,
		})
	}
	return rows, err
}

type tx struct {
	wrapped      driver.Tx
	commitHook   func(info CommitInfo)
	rollbackHook func(info RollbackInfo)
}

func (tx *tx) Commit() error {
	start := time.Now()
	err := tx.wrapped.Commit()
	if tx.commitHook != nil {
		tx.commitHook(CommitInfo{Duration: time.Since(start), Err: err})
	}
	return err
}

func (tx *tx) Rollback() error {
	start := time.Now()
	err := tx.wrapped.Rollback()
	if tx.rollbackHook != nil {
		tx.rollbackHook(RollbackInfo{Duration: time.Since(start), Err: err})
	}
	return err
}

type stmt struct {
	wrapped   driver.Stmt
	query     string
	execHook  func(ctx context.Context, info ExecInfo)
	queryHook func(ctx context.Context, info QueryInfo)
}

func (s *stmt) Close() error { return s.wrapped.Close() }
func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	start := time.Now()
	res, err := s.wrapped.Exec(args)
	if s.execHook != nil {
		s.execHook(context.Background(), ExecInfo{
			Query:    s.query,
			Args:     args,
			Duration: time.Since(start),
			Err:      err,
		})
	}
	return res, err
}
func (s *stmt) NumInput() int { return s.wrapped.NumInput() }
func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	start := time.Now()
	res, err := s.wrapped.Query(args)
	if s.queryHook != nil {
		s.queryHook(context.Background(), QueryInfo{
			Query:    s.query,
			Args:     args,
			Duration: time.Since(start),
			Err:      err,
		})
	}
	return res, err
}

var (
	_ driver.StmtExecContext  = &stmt{}
	_ driver.StmtQueryContext = &stmt{}

	// _ driver.ColumnConverter = &stmt{}
	// _ driver.NamedValueChecker = &stmt{}
)

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	start := time.Now()
	res, err := s.wrapped.(driver.StmtExecContext).ExecContext(ctx, args)
	if s.execHook != nil {
		values := make([]driver.Value, 0, len(args))
		for i := range args {
			values = append(values, args[i].Value)
		}
		s.execHook(ctx, ExecInfo{
			Query:    s.query,
			Args:     values,
			Duration: time.Since(start),
			Err:      err,
		})
	}
	return res, err
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	start := time.Now()
	var (
		rows driver.Rows
		err  error
	)

	values := make([]driver.Value, 0, len(args))
	for i := range args {
		values = append(values, args[i].Value)
	}
	sqc, ok := s.wrapped.(driver.StmtQueryContext)
	if ok {
		rows, err = sqc.QueryContext(ctx, args)
	} else {
		rows, err = s.wrapped.Query(values)
	}
	if s.queryHook != nil {
		s.queryHook(ctx, QueryInfo{
			Query:    s.query,
			Args:     values,
			Duration: time.Since(start),
			Err:      err,
		})
	}
	return rows, err
}
