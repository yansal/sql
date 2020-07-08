package build

// Join returns a new from item with a JOIN clause.
func Join(left Expression, right Expression) JoinExpr {
	return joinExpr{left: left, right: right}
}

// LeftJoin returns a new from item with a LEFT JOIN clause.
func LeftJoin(left Expression, right Expression) JoinExpr {
	return joinExpr{jointype: "LEFT", left: left, right: right}
}

// A JoinExpr is a join expression.
type JoinExpr interface {
	Expression
	On(on Expression) Expression
}

type joinExpr struct {
	jointype        string
	left, right, on Expression
}

func (j joinExpr) On(on Expression) Expression {
	j.on = on
	return j
}

func (j joinExpr) build(b *builder) {
	j.left.build(b)
	if j.jointype != "" {
		b.write(" " + j.jointype)
	}
	b.write(" JOIN ")
	j.right.build(b)
	if j.on != nil {
		b.write(" ON ")
		j.on.build(b)
	}
}
