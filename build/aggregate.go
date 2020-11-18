package build

// Aggr returns a new AggrExpression.
func Aggr(name string, exprs ...Expression) AggrExpression {
	return aggrExpression{
		name:  name,
		exprs: exprs,
	}
}

// AggrDistinct returns a new AggrExpression with the DISTINCT keyword.
func AggrDistinct(name string, exprs ...Expression) AggrExpression {
	return aggrExpression{
		name:     name,
		distinct: true,
		exprs:    exprs,
	}
}

// An AggrExpression is an aggregate expression.
type AggrExpression interface {
	Expression
	OrderBy(Expression) AggrExpression
	FilterWhere(Expression) Expression
}

type aggrExpression struct {
	name        string
	distinct    bool
	exprs       []Expression
	orderby     Expression
	filterwhere Expression
}

func (a aggrExpression) OrderBy(expr Expression) AggrExpression {
	a.orderby = expr
	return a
}

func (a aggrExpression) FilterWhere(expr Expression) Expression {
	a.filterwhere = expr
	return a
}

func (a aggrExpression) build(b *builder) {
	b.write(a.name)
	b.write("(")
	if a.distinct {
		b.write("DISTINCT ")
	}
	for i, expr := range a.exprs {
		if i > 0 {
			b.write(", ")
		}
		expr.build(b)
	}
	if a.orderby != nil {
		b.write(" ORDER BY ")
		a.orderby.build(b)
	}
	b.write(")")
	if a.filterwhere != nil {
		b.write(" FILTER (WHERE ")
		a.filterwhere.build(b)
		b.write(")")
	}
}
