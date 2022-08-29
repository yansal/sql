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
](ctx context.Context, db Querier, options ...FindOption) ([]T, error) {
	var dest []T
	if err := find[T, PtrToT](ctx, db, &dest, options...); err != nil {
		return nil, err
	}
	return dest, nil
}

type FindOption func(o *FindOptions)
type FindOptions struct {
	joins  []FindOptionsJoin
	where  build.Expression
	orders []build.Expression
	limit  *int
	offset *int
}

type FindOptionsJoin struct {
	Left  bool
	Right build.Expression
	On    build.Expression
}

func WithLimit(limit int) FindOption {
	return func(o *FindOptions) { o.limit = &limit }
}
func WithOffset(offset int) FindOption {
	return func(o *FindOptions) { o.offset = &offset }
}
func WithJoins(joins []FindOptionsJoin) FindOption {
	return func(o *FindOptions) { o.joins = joins }
}
func WithOrders(orders []build.Expression) FindOption {
	return func(o *FindOptions) { o.orders = orders }
}
func WithWhere(where build.Expression) FindOption {
	return func(o *FindOptions) { o.where = where }
}

func find[
	T any,
	PtrToT interface {
		*T
		Model
	},
](ctx context.Context, db Querier, dest *[]T, options ...FindOption) error {
	var opts FindOptions
	for i := range options {
		options[i](&opts)
	}
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
	for i := range opts.joins {
		joinexpr := fromitem.Join(opts.joins[i].Right)
		if opts.joins[i].Left {
			joinexpr = fromitem.LeftJoin(opts.joins[i].Right)
		}
		fromitem = joinexpr.On(opts.joins[i].On)
	}
	stmt = stmt.From(fromitem)
	if opts.where != nil {
		stmt = stmt.Where(opts.where)
	}
	if opts.orders != nil {
		stmt = stmt.OrderBy(opts.orders...)
	}
	if opts.limit != nil {
		stmt = stmt.Limit(build.Bind(*opts.limit))
	}
	if opts.offset != nil {
		stmt = stmt.Offset(build.Bind(*opts.offset))
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
