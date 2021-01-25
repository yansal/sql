package build

// FromItem returns a new FROM item.
func FromItem(expr Expression) FromItemExpr {
	return &fromItemExpr{expr: expr}
}

// A FromItemExpr is a FROM item expression.
type FromItemExpr interface {
	Expression
	Join(expr Expression) JoinExpr
	LeftJoin(expr Expression) JoinExpr
}

type fromItemExpr struct {
	expr Expression
}

func (e *fromItemExpr) Join(right Expression) JoinExpr {
	return &joinExpr{left: e, right: right}
}

func (e *fromItemExpr) LeftJoin(right Expression) JoinExpr {
	return &joinExpr{left: e, jointype: "LEFT", right: right}
}

func (e *fromItemExpr) build(b *builder) {
	e.expr.build(b)
}

// A JoinExpr is a FROM item expression with a JOIN.
type JoinExpr interface {
	FromItemExpr
	On(on Expression) FromItemExpr
}

type joinExpr struct {
	jointype        string
	left, right, on Expression
}

func (e *joinExpr) LeftJoin(right Expression) JoinExpr {
	return &joinExpr{left: e, jointype: "LEFT", right: right}
}

func (e *joinExpr) Join(right Expression) JoinExpr {
	return &joinExpr{left: e, right: right}
}

func (e *joinExpr) On(on Expression) FromItemExpr {
	e.on = on
	return e
}

func (e *joinExpr) build(b *builder) {
	e.left.build(b)
	if e.jointype != "" {
		b.write(" " + e.jointype)
	}
	b.write(" JOIN ")
	e.right.build(b)
	if e.on != nil {
		b.write(" ON ")
		e.on.build(b)
	}
}
