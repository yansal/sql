package build

import "fmt"

// Select returns a new SELECT command.
func Select(exprs ...Expression) *SelectCmd {
	return &SelectCmd{exprs: exprs}
}

// From adds a FROM clause.
func (cmd *SelectCmd) From(items ...Expression) *SelectCmd {
	cmd.from = items
	return cmd
}

// Where adds a WHERE clause.
func (cmd *SelectCmd) Where(condition Expression) *SelectCmd {
	cmd.where = &where{Expression: condition}
	return cmd
}

// GroupBy adds a GROUP BY clause.
func (cmd *SelectCmd) GroupBy(elements ...Expression) *SelectCmd {
	cmd.groupby = elements
	return cmd
}

// OrderBy adds a ORDER BY clause.
func (cmd *SelectCmd) OrderBy(exprs ...Expression) *SelectCmd {
	cmd.orderby = exprs
	return cmd
}

// Limit adds a LIMIT clause.
func (cmd *SelectCmd) Limit(count Expression) *SelectCmd {
	cmd.limit = &limit{Expression: count}
	return cmd
}

// Offset adds a OFFSET clause.
func (cmd *SelectCmd) Offset(start Expression) *SelectCmd {
	cmd.offset = &offset{Expression: start}
	return cmd
}

// Build builds cmd and its parameters.
func (cmd *SelectCmd) Build() (string, []interface{}) {
	b := new(builder)
	cmd.build(b)
	return b.buf.String(), b.params
}

func (cmd *SelectCmd) build(b *builder) {
	if cmd.ctes != nil {
		cmd.ctes.build(b)
	}
	b.write("SELECT ")
	cmd.exprs.build(b)

	if cmd.from != nil {
		b.write(" ")
		cmd.from.build(b)
	}

	if cmd.where != nil {
		b.write(" ")
		cmd.where.build(b)
	}

	if cmd.groupby != nil {
		b.write(" ")
		cmd.groupby.build(b)
	}

	if cmd.orderby != nil {
		b.write(" ")
		cmd.orderby.build(b)
	}

	if cmd.limit != nil {
		b.write(" ")
		cmd.limit.build(b)
	}

	if cmd.offset != nil {
		b.write(" ")
		cmd.offset.build(b)
	}
}

// A SelectCmd is a SELECT command.
type SelectCmd struct {
	ctes    *CTEs
	exprs   selectexprs
	from    from
	where   *where
	groupby groupby
	orderby orderby
	limit   *limit
	offset  *offset
}

type selectexprs []Expression

func (exprs selectexprs) build(b *builder) {
	for i := range exprs {
		if i > 0 {
			b.write(", ")
		}
		exprs[i].build(b)
	}
}

func ColumnExpr(expr Expression) AsExpr {
	return asExpr{expr: expr}
}

type AsExpr interface {
	Expression
	As(string) Expression
}

type asExpr struct {
	expr  Expression
	alias identifier
}

func (e asExpr) As(alias string) Expression {
	return asExpr{expr: e.expr, alias: identifier(alias)}
}

func (e asExpr) build(b *builder) {
	if _, ok := e.expr.(*SelectCmd); ok {
		b.write("(")
		e.expr.build(b)
		b.write(")")
	} else {
		e.expr.build(b)
	}
	if e.alias != "" {
		b.write(" AS ")
		e.alias.build(b)
	}
}

func Columns(names ...string) []Expression {
	var columns []Expression
	for _, name := range names {
		columns = append(columns, asExpr{expr: identifier(name)})
	}
	return columns
}

type from []Expression

func (f from) build(b *builder) {
	b.write("FROM ")
	for i := range f {
		if i > 0 {
			b.write(", ")
		}
		f[i].build(b)
	}
}

func FromExpr(expr Expression) AsExpr {
	return asExpr{expr: expr}
}

type where struct{ Expression }

func (w where) build(b *builder) {
	b.write("WHERE ")
	w.Expression.build(b)
}

type groupby []Expression

func (g groupby) build(b *builder) {
	b.write("GROUP BY ")
	for i := range g {
		if i > 0 {
			b.write(", ")
		}
		g[i].build(b)
	}
}

type orderby []Expression

func (o orderby) build(b *builder) {
	b.write("ORDER BY ")
	for i := range o {
		if i > 0 {
			b.write(", ")
		}
		o[i].build(b)
	}
}

type limit struct{ Expression }

func (l limit) build(b *builder) {
	b.write("LIMIT ")
	l.Expression.build(b)
}

type offset struct{ Expression }

func (l offset) build(b *builder) {
	b.write("OFFSET ")
	l.Expression.build(b)
}

// Order returns a new OrderExpr.
func Order(expr Expression, direction Direction) OrderExpr {
	return &orderExpr{expr: expr, direction: &direction}
}

// An OrderExpr is a ORDER BY expression.
type OrderExpr interface {
	Expression
	Nulls(Nulls) Expression
}

type orderExpr struct {
	expr      Expression
	direction *Direction
	nulls     *Nulls
}

func (o orderExpr) Nulls(nulls Nulls) Expression {
	o.nulls = &nulls
	return o
}

func (o orderExpr) build(b *builder) {
	o.expr.build(b)

	if o.direction != nil {
		switch d := *o.direction; d {
		case Asc:
			b.write(" ASC")
		case Desc:
			b.write(" DESC")
		default:
			panic(fmt.Sprintf("unknown direction %d", d))
		}
	}

	if o.nulls != nil {
		switch n := *o.nulls; n {
		case First:
			b.write(" NULLS FIRST")
		case Last:
			b.write(" NULLS LAST")
		default:
			panic(fmt.Sprintf("unknown nulls %d", n))
		}
	}
}

// A Direction is an ORDER BY direction.
type Direction int

// Direction values.
const (
	Asc Direction = iota
	Desc
)

// A Nulls is a NULLS option in an ORDER BY clause.
type Nulls int

// Nulls values.
const (
	First Nulls = iota
	Last
)
