package build

// Select returns a new select command.
func Select(exprs ...Expression) *SelectCmd {
	return &SelectCmd{exprs: exprs}
}

// From adds a from clause.
func (stmt *SelectCmd) From(items ...Expression) *SelectCmd {
	stmt.from = items
	return stmt
}

// Where adds a where clause.
func (stmt *SelectCmd) Where(condition Expression) *SelectCmd {
	stmt.where = &where{Expression: condition}
	return stmt
}

// GroupBy adds a group by clause.
func (stmt *SelectCmd) GroupBy(elements ...Expression) *SelectCmd {
	stmt.groupby = elements
	return stmt
}

// OrderBy adds a order by clause.
func (stmt *SelectCmd) OrderBy(exprs ...Expression) *SelectCmd {
	stmt.orderby = exprs
	return stmt
}

// Limit adds a limit clause.
func (stmt *SelectCmd) Limit(count Expression) *SelectCmd {
	stmt.limit = &limit{Expression: count}
	return stmt
}

// Offset adds a offset clause.
func (stmt *SelectCmd) Offset(start Expression) *SelectCmd {
	stmt.offset = &offset{Expression: start}
	return stmt
}

// Build builds stmt and its parameters.
func (stmt *SelectCmd) Build() (string, []interface{}) {
	b := new(builder)
	stmt.build(b)
	return b.buf.String(), b.params
}

func (stmt *SelectCmd) build(b *builder) {
	b.write("SELECT ")
	stmt.exprs.build(b)

	if stmt.from != nil {
		b.write(" ")
		stmt.from.build(b)
	}

	if stmt.where != nil {
		b.write(" ")
		stmt.where.build(b)
	}

	if stmt.groupby != nil {
		b.write(" ")
		stmt.groupby.build(b)
	}

	if stmt.orderby != nil {
		b.write(" ")
		stmt.orderby.build(b)
	}

	if stmt.limit != nil {
		b.write(" ")
		stmt.limit.build(b)
	}

	if stmt.offset != nil {
		b.write(" ")
		stmt.offset.build(b)
	}
}

// A SelectCmd is a select command.
type SelectCmd struct {
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

// Join returns a new from item with a join clause.
func Join(left Expression, right Expression, condition Expression) Expression {
	return &join{left: left, right: right, condition: condition}
}

type join struct {
	left, right, condition Expression
}

func (i *join) build(b *builder) {
	i.left.build(b)
	b.write(" JOIN ")
	i.right.build(b)
	b.write(" ON ")
	i.condition.build(b)
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

func OrderExpr(expr Expression, ordering string) Expression {
	return &orderExpr{expr: expr, ordering: ordering}
}

type orderExpr struct {
	expr     Expression
	ordering string
}

func (o orderExpr) build(b *builder) {
	o.expr.build(b)
	if o.ordering != "" {
		b.write(" " + o.ordering)
	}
}

const (
	Asc  = "ASC"
	Desc = "DESC"
)
