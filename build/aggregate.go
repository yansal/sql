package build

// Aggr returns a new AggrExpression.
func Aggr(expr Expression) AggrExpression {
	return aggrExpression{expr: expr}
}

// An AggrExpression is an aggregate expression.
type AggrExpression interface {
	FilterWhere(Expression) Expression
}

type aggrExpression struct {
	expr        Expression
	filterwhere Expression
}

func (a aggrExpression) FilterWhere(expr Expression) Expression {
	a.filterwhere = expr
	return a
}

func (a aggrExpression) build(b *builder) {
	a.expr.build(b)
	if a.filterwhere != nil {
		b.write(" FILTER (WHERE ")
		a.filterwhere.build(b)
		b.write(")")
	}
}
