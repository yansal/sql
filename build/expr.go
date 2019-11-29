package build

// Bind binds a value.
func Bind(value interface{}) interface{} {
	return &bind{value: value}
}

type bind struct{ value interface{} }

func (bind *bind) build(b *builder) {
	b.bind(bind.value)
}

// Expr starts a new infix expression.
func Expr(left interface{}) *InfixExpr {
	return &InfixExpr{left: left}
}

// An InfixExpr is an infix expression.
type InfixExpr struct {
	left  interface{}
	op    string
	right interface{}
}

func (i *InfixExpr) build(b *builder) {
	b.build(i.left)
	if i.op == "" || i.right == nil {
		return
	}

	b.write(" " + i.op + " ")
	b.build(i.right)
}

// In invokes the IN operator.
func (i *InfixExpr) In(right interface{}) *InfixExpr {
	return i.Op("IN", right)
}

// Equal invokes the = operator.
func (i *InfixExpr) Equal(right interface{}) *InfixExpr {
	return i.Op("=", right)
}

// Op invokes an operator.
func (i *InfixExpr) Op(op string, right interface{}) *InfixExpr {
	return &InfixExpr{left: &InfixExpr{left: i.left, op: op, right: right}}
}
