package build

// Select returns a new select statement.
func Select(exprs ...interface{}) *SelectStmt {
	return &SelectStmt{exprs: exprs}
}

// From adds a from clause.
func (stmt *SelectStmt) From(items ...interface{}) *SelectStmt {
	stmt.from = &from{items: items}
	return stmt
}

// Where adds a where clause.
func (stmt *SelectStmt) Where(condition interface{}) *SelectStmt {
	stmt.where = &where{condition: condition}
	return stmt
}

// GroupBy adds a group by clause.
func (stmt *SelectStmt) GroupBy(elements ...interface{}) *SelectStmt {
	stmt.groupby = &groupby{elements: elements}
	return stmt
}

// OrderBy adds a order by clause.
func (stmt *SelectStmt) OrderBy(exprs ...interface{}) *SelectStmt {
	stmt.orderby = &orderby{exprs: exprs}
	return stmt
}

// Build builds stmt and its parameters.
func (stmt *SelectStmt) Build() (string, []interface{}) {
	b := new(builder)
	stmt.build(b)
	return b.buf.String(), b.params
}

func (stmt *SelectStmt) build(b *builder) {
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
}

// A SelectStmt is a select statement.
type SelectStmt struct {
	exprs   selectexprs
	from    *from
	where   *where
	groupby *groupby
	orderby *orderby
}

type selectexprs []interface{}

func (exprs selectexprs) build(b *builder) {
	for i := range exprs {
		if i > 0 {
			b.write(", ")
		}
		b.build(exprs[i])
	}
}

type from struct{ items []interface{} }

func (f from) build(b *builder) {
	b.write("FROM ")
	for i := range f.items {
		if i > 0 {
			b.write(", ")
		}
		b.build(f.items[i])
	}
}

// Join returns a new from item with a join clause.
func Join(left interface{}, right interface{}, condition interface{}) interface{} {
	return &join{left: left, right: right, condition: condition}
}

// A join is a from item with a join clause.
type join struct {
	left      interface{}
	right     interface{}
	condition interface{}
}

func (i *join) build(b *builder) {
	b.build(i.left)

	if i.right == nil || i.condition == nil {
		return
	}

	b.write(" JOIN ")
	b.build(i.right)
	b.write(" ON ")
	b.build(i.condition)
}

type where struct{ condition interface{} }

func (w where) build(b *builder) {
	b.write("WHERE ")
	b.build(w.condition)
}

type groupby struct{ elements []interface{} }

func (g groupby) build(b *builder) {
	b.write("GROUP BY ")
	for i := range g.elements {
		if i > 0 {
			b.write(", ")
		}
		b.build(g.elements[i])
	}
}

type orderby struct{ exprs []interface{} }

func (o orderby) build(b *builder) {
	b.write("ORDER BY ")
	for i := range o.exprs {
		if i > 0 {
			b.write(", ")
		}
		b.build(o.exprs[i])
	}
}

type order struct {
	expr     interface{}
	ordering string
}

func ColumnExpr(expr interface{}) *AsExpr {
	return &AsExpr{expr: expr}
}
func FromExpr(expr interface{}) *AsExpr {
	return &AsExpr{expr: expr}
}

type AsExpr struct {
	expr  interface{}
	alias string
}

func (a *AsExpr) As(alias string) interface{} {
	return &AsExpr{expr: a.expr, alias: alias}
}

func (a AsExpr) build(b *builder) {
	b.build(a.expr)
	b.write(" AS " + a.alias)
}

func OrderExpr(expr interface{}, ordering string) *orderExpr {
	return &orderExpr{expr: expr, ordering: ordering}
}

type orderExpr struct {
	expr     interface{}
	ordering string
}

func (o orderExpr) build(b *builder) {
	b.build(o.expr)
	if o.ordering != "" {
		b.write(" " + o.ordering)

	}
}

const (
	ASC  = "ASC"
	Desc = "DESC"
)
