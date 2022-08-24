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
	Joins  []FindQueryJoin
	Where  build.Expression
	Orders []build.Expression
	Limit  *int
}

type FindQueryJoin struct {
	Left  bool
	Right build.Expression
	On    build.Expression
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
	for i := range columns {
		columns[i] = table + "." + columns[i]
	}
	stmt := build.Select(build.Columns(columns...)...)
	fromitem := build.FromItem(build.Ident(table))
	for i := range q.Joins {
		joinexpr := fromitem.Join(q.Joins[i].Right)
		if q.Joins[i].Left {
			joinexpr = fromitem.LeftJoin(q.Joins[i].Right)
		}
		fromitem = joinexpr.On(q.Joins[i].On)
	}
	stmt = stmt.From(fromitem)
	if q.Where != nil {
		stmt = stmt.Where(q.Where)
	}
	if q.Orders != nil {
		stmt = stmt.OrderBy(q.Orders...)
	}
	if q.Limit != nil {
		stmt = stmt.Limit(build.Bind(*q.Limit))
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
