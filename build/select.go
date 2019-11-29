package build

// Select returns a new select command.
func Select(exprs ...interface{}) *SelectCmd {
	return &SelectCmd{exprs: exprs}
}

// From adds a from clause.
func (stmt *SelectCmd) From(items ...interface{}) *SelectCmd {
	stmt.from = &from{items: items}
	return stmt
}

// Where adds a where clause.
func (stmt *SelectCmd) Where(condition interface{}) *SelectCmd {
	stmt.where = &where{condition: condition}
	return stmt
}

// GroupBy adds a group by clause.
func (stmt *SelectCmd) GroupBy(elements ...interface{}) *SelectCmd {
	stmt.groupby = &groupby{elements: elements}
	return stmt
}

// OrderBy adds a order by clause.
func (stmt *SelectCmd) OrderBy(exprs ...interface{}) *SelectCmd {
	stmt.orderby = &orderby{exprs: exprs}
	return stmt
}

// Limit adds a limit clause.
func (stmt *SelectCmd) Limit(count interface{}) *SelectCmd {
	stmt.limit = &limit{count: count}
	return stmt
}

// Offset adds a offset clause.
func (stmt *SelectCmd) Offset(start interface{}) *SelectCmd {
	stmt.offset = &offset{start: start}
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

// A SelectCmd is a select statement.
type SelectCmd struct {
	exprs   selectexprs
	from    *from
	where   *where
	groupby *groupby
	orderby *orderby
	limit   *limit
	offset  *offset
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

type limit struct{ count interface{} }

func (l limit) build(b *builder) {
	b.write("LIMIT ")
	b.build(l.count)
}

type offset struct{ start interface{} }

func (l offset) build(b *builder) {
	b.write("OFFSET ")
	b.build(l.start)
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

func OrderExpr(expr interface{}, ordering string) interface{} {
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
	Asc  = "ASC"
	Desc = "DESC"
)
