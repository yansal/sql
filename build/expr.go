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

	if i.op == "" {
		return
	}
	b.write(" " + i.op)

	if i.right == nil {
		return
	}
	b.write(" ")
	b.build(i.right)
}

// And invokes the AND operator.
func (i *InfixExpr) And(right interface{}) *InfixExpr {
	return i.Op("AND", right)
}

// Equal invokes the = operator.
func (i *InfixExpr) Equal(right interface{}) *InfixExpr {
	return i.Op("=", right)
}

// In invokes the IN operator.
func (i *InfixExpr) In(right interface{}) *InfixExpr {
	return i.Op("IN", right)
}

// LessThan invokes the < operator.
func (i *InfixExpr) LessThan(right interface{}) *InfixExpr {
	return i.Op("<", right)
}

// GreaterThan invokes the > operator.
func (i *InfixExpr) GreaterThan(right interface{}) *InfixExpr {
	return i.Op(">", right)
}

// GreaterThanOrEqualTo invokes the >= operator.
func (i *InfixExpr) GreaterThanOrEqualTo(right interface{}) *InfixExpr {
	return i.Op(">=", right)
}

// Op invokes an operator.
func (i *InfixExpr) Op(op string, right interface{}) *InfixExpr {
	return &InfixExpr{left: &InfixExpr{left: i.left, op: op, right: right}}
}

func (i *InfixExpr) IsNull() *InfixExpr {
	return &InfixExpr{left: &InfixExpr{left: i.left, op: "IS NULL"}}
}

func CallExpr(function string, args ...interface{}) interface{} {
	return &callExpr{function: function, args: args}
}

type callExpr struct {
	function string
	args     []interface{}
}

func (e callExpr) build(b *builder) {
	b.write(e.function)
	b.write("(")
	for i, arg := range e.args {
		if i > 0 {
			b.write(", ")
		}
		b.build(arg)
	}
	b.write(")")
}
