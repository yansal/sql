package build

// CaseWhen returns a new CASE clause.
func CaseWhen(condition Expression, result Expression) CaseExpr {
	return CaseExpr{whens: []casewhen{{condition: condition, result: result}}}
}

// A CaseExpr is a CASE expression.
type CaseExpr struct {
	whens      []casewhen
	elseresult Expression
}

type casewhen struct {
	condition Expression
	result    Expression
}

// When adds a WHEN clause.
func (c CaseExpr) When(condition Expression, result Expression) CaseExpr {
	c.whens = append(c.whens, casewhen{condition: condition, result: result})
	return c
}

// Else adds a ELSE clause.
func (c CaseExpr) Else(result Expression) Expression {
	c.elseresult = result
	return c
}

func (c CaseExpr) build(b *builder) {
	b.write("CASE")
	for i := range c.whens {
		b.write(" WHEN ")
		c.whens[i].condition.build(b)
		b.write(" THEN ")
		c.whens[i].result.build(b)
	}
	if c.elseresult != nil {
		b.write(" ELSE ")
		c.elseresult.build(b)
	}
	b.write(" END")
}
