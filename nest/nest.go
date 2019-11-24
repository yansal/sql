package nest

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

// Querier runs sql commands, with support for nested transactions.
type Querier interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)

	BeginTx(context.Context, *sql.TxOptions) (Querier, error)
	Commit() error
	Rollback() error
}

var (
	_ Querier = &db{}
	_ Querier = &tx{}
)

// Wrap returns a new Querier wrapping d.
func Wrap(d *sql.DB) Querier { return &db{db: d} }

type db struct{ db *sql.DB }

func (d *db) BeginTx(ctx context.Context, opts *sql.TxOptions) (Querier, error) {
	t, err := d.db.BeginTx(ctx, opts)
	if err != nil {
		return nil, err
	}
	return &tx{ctx: ctx, tx: t}, nil
}

func (d *db) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return d.db.ExecContext(ctx, query, args...)
}

func (d *db) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return d.db.QueryContext(ctx, query, args...)
}

var errNoTx = errors.New("sql/nest: not in a transaction")

func (d *db) Commit() error   { return errNoTx }
func (d *db) Rollback() error { return errNoTx }

type tx struct {
	ctx       context.Context
	tx        *sql.Tx
	index     int
	savepoint string
}

func (t *tx) BeginTx(ctx context.Context, opts *sql.TxOptions) (Querier, error) {
	index := t.index + 1
	savepoint := fmt.Sprintf("s%d", index)

	_, err := t.tx.ExecContext(t.ctx, "SAVEPOINT "+savepoint)
	if err != nil {
		return nil, err
	}
	return &tx{ctx: ctx, tx: t.tx, savepoint: savepoint, index: index}, nil
}

func (t *tx) ExecContext(ctx context.Context, query string, args ...interface{}) (sql.Result, error) {
	return t.tx.ExecContext(ctx, query, args...)
}

func (t *tx) QueryContext(ctx context.Context, query string, args ...interface{}) (*sql.Rows, error) {
	return t.tx.QueryContext(ctx, query, args...)
}

func (t *tx) Commit() error {
	if len(t.savepoint) == 0 {
		return t.tx.Commit()
	}
	_, err := t.tx.ExecContext(t.ctx, "RELEASE SAVEPOINT "+t.savepoint)
	return err
}

func (t *tx) Rollback() error {
	if len(t.savepoint) == 0 {
		return t.tx.Rollback()
	}
	_, err := t.tx.ExecContext(t.ctx, "ROLLBACK TO SAVEPOINT "+t.savepoint)
	return err
}
