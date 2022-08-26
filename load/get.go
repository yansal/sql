package load

import (
	"context"
	"database/sql"

	"github.com/yansal/sql/build"
)

func Get[
	T any,
	PtrToT interface {
		*T
		Model
	},
](ctx context.Context, db Querier, where build.Expression) (PtrToT, error) {
	var (
		dest    T
		destptr PtrToT = &dest
	)
	if err := get(ctx, db, destptr, where); err != nil {
		return nil, err
	}
	return destptr, nil
}

func get(ctx context.Context, db Querier, dest Model, where build.Expression) error {
	var (
		columns = dest.GetColumns()
		table   = dest.GetTable()
	)
	query, args := build.Select(build.Columns(columns...)...).
		From(build.Ident(table)).
		Where(where).
		Build()

	rows, err := db.QueryContext(ctx, query, args...)
	if err != nil {
		return err
	}
	defer rows.Close()

	if !rows.Next() {
		return sql.ErrNoRows
	}
	if err := rows.Scan(dest.GetDests()...); err != nil {
		return err
	}
	if err := rows.Close(); err != nil {
		return err
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}
