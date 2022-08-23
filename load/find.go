package load

import (
	"context"

	"github.com/yansal/sql/build"
)

func Find[
	T any,
	PtrToT interface {
		*T
		Model
	},
](ctx context.Context, db Querier, where build.Expression) ([]T, error) {
	var dest []T
	if err := find[T, PtrToT](ctx, db, &dest, where); err != nil {
		return nil, err
	}
	return dest, nil
}

func find[
	T any,
	PtrToT interface {
		*T
		Model
	},
](ctx context.Context, db Querier, dest *[]T, where build.Expression) error {
	var (
		model   PtrToT
		columns = model.GetColumns()
		table   = model.GetTable()
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

	for rows.Next() {
		var (
			v    T
			vptr PtrToT = &v
		)
		if err := rows.Scan(vptr.GetDests()...); err != nil {
			return err
		}
		*dest = append(*dest, v)
	}
	if err := rows.Err(); err != nil {
		return err
	}
	return nil
}
