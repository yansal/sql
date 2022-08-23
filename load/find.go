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
](ctx context.Context, db Querier, q *FindQuery) ([]T, error) {
	var dest []T
	if err := find[T, PtrToT](ctx, db, &dest, q); err != nil {
		return nil, err
	}
	return dest, nil
}

type FindQuery struct {
	where  build.Expression
	orders []build.Expression
	limit  *int
}

func find[
	T any,
	PtrToT interface {
		*T
		Model
	},
](ctx context.Context, db Querier, dest *[]T, q *FindQuery) error {
	var (
		model   PtrToT
		columns = model.GetColumns()
		table   = model.GetTable()
	)
	stmt := build.Select(build.Columns(columns...)...).
		From(build.Ident(table))
	if q.where != nil {
		stmt = stmt.Where(q.where)
	}
	if q.orders != nil {
		stmt = stmt.OrderBy(q.orders...)
	}
	if q.limit != nil {
		stmt = stmt.Limit(build.Bind(*q.limit))
	}
	query, args := stmt.Build()

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