package build

// With returns a new CTEs.
func With(alias string, stmt Expression) *CTEs {
	return &CTEs{ctes: []cte{{alias: alias, stmt: stmt}}}
}

// A CTEs is a list of Common Table Expressions.
type CTEs struct {
	ctes []cte
}

type cte struct {
	alias string
	stmt  Expression
}

// With appends a new Common Table Expressions to e.
func (e *CTEs) With(alias string, stmt Expression) *CTEs {
	e.ctes = append(e.ctes, cte{alias: alias, stmt: stmt})
	return e
}

// Select starts a new select statement attached to e.
func (e *CTEs) Select(exprs ...Expression) *SelectCmd {
	return &SelectCmd{ctes: e, exprs: exprs}
}

func (e *CTEs) build(b *builder) {
	b.write("WITH ")
	for i, cte := range e.ctes {
		if i > 0 {
			b.write(", ")
		}
		b.write(cte.alias)
		b.write(" AS ( ")
		cte.stmt.build(b)
		b.write(" )")
	}
	b.write(" ")
}
