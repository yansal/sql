package build

import "fmt"

// Select returns a new SELECT statement.
func Select(exprs ...Expression) *SelectStmt {
	return &SelectStmt{exprs: exprs}
}

// DistinctOn adds a DISINCT ON clause.
func (s *SelectStmt) DistinctOn(exprs ...Expression) *SelectStmt {
	s.distincton = exprs
	return s
}

// From adds a FROM clause.
func (s *SelectStmt) From(items ...Expression) *SelectStmt {
	s.from = items
	return s
}

// Where adds a WHERE clause.
func (s *SelectStmt) Where(condition Expression) *SelectStmt {
	s.where = &where{Expression: condition}
	return s
}

// GroupBy adds a GROUP BY clause.
func (s *SelectStmt) GroupBy(elements ...Expression) *SelectStmt {
	s.groupby = elements
	return s
}

// Union adds a UNION clause.
func (s *SelectStmt) Union(stmt Expression) *SelectStmt {
	s.unions = append(s.unions, stmt)
	return s
}

// OrderBy adds a ORDER BY clause.
func (s *SelectStmt) OrderBy(exprs ...Expression) *SelectStmt {
	s.orderby = exprs
	return s
}

// Limit adds a LIMIT clause.
func (s *SelectStmt) Limit(count Expression) *SelectStmt {
	s.limit = &limit{Expression: count}
	return s
}

// Offset adds a OFFSET clause.
func (s *SelectStmt) Offset(start Expression) *SelectStmt {
	s.offset = &offset{Expression: start}
	return s
}

// Build builds s and its parameters.
func (s *SelectStmt) Build() (string, []interface{}) {
	b := new(builder)
	s.build(b)
	return b.buf.String(), b.params
}

func (s *SelectStmt) build(b *builder) {
	if s.ctes != nil {
		s.ctes.build(b)
	}

	b.write("SELECT ")

	if s.distincton != nil {
		s.distincton.build(b)
	}

	s.exprs.build(b)

	if s.from != nil {
		b.write(" ")
		s.from.build(b)
	}

	if s.where != nil {
		b.write(" ")
		s.where.build(b)
	}

	if s.groupby != nil {
		b.write(" ")
		s.groupby.build(b)
	}

	if s.unions != nil {
		b.write(" ")
		s.unions.build(b)
	}

	if s.orderby != nil {
		b.write(" ")
		s.orderby.build(b)
	}

	if s.limit != nil {
		b.write(" ")
		s.limit.build(b)
	}

	if s.offset != nil {
		b.write(" ")
		s.offset.build(b)
	}
}

// A SelectStmt is a SELECT statement.
type SelectStmt struct {
	ctes       *CTEs
	distincton distincton
	exprs      selectexprs
	from       from
	where      *where
	groupby    groupby
	unions     unions
	orderby    orderby
	limit      *limit
	offset     *offset
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
	if _, ok := e.expr.(*SelectStmt); ok {
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

type distincton []Expression

func (exprs distincton) build(b *builder) {
	b.write("DISTINCT ON (")
	for i := range exprs {
		if i > 0 {
			b.write(", ")
		}
		exprs[i].build(b)
	}
	b.write(") ")
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

type unions []Expression

func (u unions) build(b *builder) {
	for i := range u {
		if i > 0 {
			b.write(" ")
		}
		b.write("UNION ")
		u[i].build(b)
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
