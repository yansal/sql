package build

import (
	"fmt"
	"strconv"
)

type Expression interface{ build(*builder) }

// Bind binds a value.
func Bind(value interface{}) Expression {
	return &bind{value: value}
}

type bind struct{ value interface{} }

func (bind *bind) build(b *builder) {
	b.bind(bind.value)
}

// Infix starts a new infix expression.
func Infix(left Expression) *InfixExpr {
	return &InfixExpr{left: left}
}

// An InfixExpr is an infix expression.
type InfixExpr struct {
	left  Expression
	op    string
	right Expression
}

func (i *InfixExpr) build(b *builder) {
	i.left.build(b)

	if i.op == "" {
		return
	}
	b.write(" " + i.op)

	if i.right == nil {
		return
	}
	b.write(" ")
	i.right.build(b)
}

// And invokes the AND operator.
func (i *InfixExpr) And(right Expression) *InfixExpr {
	return i.Op("AND", right)
}

// Equal invokes the = operator.
func (i *InfixExpr) Equal(right Expression) *InfixExpr {
	return i.Op("=", right)
}

// In invokes the IN operator.
func (i *InfixExpr) In(right Expression) *InfixExpr {
	return i.Op("IN", right)
}

// LessThan invokes the < operator.
func (i *InfixExpr) LessThan(right Expression) *InfixExpr {
	return i.Op("<", right)
}

// GreaterThan invokes the > operator.
func (i *InfixExpr) GreaterThan(right Expression) *InfixExpr {
	return i.Op(">", right)
}

// GreaterThanOrEqualTo invokes the >= operator.
func (i *InfixExpr) GreaterThanOrEqualTo(right Expression) *InfixExpr {
	return i.Op(">=", right)
}

// Op invokes an operator.
func (i *InfixExpr) Op(op string, right Expression) *InfixExpr {
	return &InfixExpr{left: &InfixExpr{left: i.left, op: op, right: right}}
}

func (i *InfixExpr) IsNull() *InfixExpr {
	return &InfixExpr{left: &InfixExpr{left: i.left, op: "IS NULL"}}
}

func CallExpr(function string, args ...Expression) Expression {
	return &callExpr{function: function, args: args}
}

type callExpr struct {
	function string
	args     []Expression
}

func (e callExpr) build(b *builder) {
	b.write(e.function)
	b.write("(")
	for i, arg := range e.args {
		if i > 0 {
			b.write(", ")
		}
		arg.build(b)
	}
	b.write(")")
}

func Identifier(s string) Expression { return identifier(s) }

type identifier string

func (i identifier) build(b *builder) {
	b.write(strconv.Quote(string(i))) // TODO: quote only if the identifier must be quoted?
}

func Int(i int) Expression { return intExpr(i) }

type intExpr int

func (i intExpr) build(b *builder) {
	b.write(fmt.Sprintf("%d", i))
}

func String(s string) Expression { return stringExpr(s) }

type stringExpr string

func (s stringExpr) build(b *builder) {
	b.write(fmt.Sprintf("'%s'", s))
}

var Star Expression = star{}

type star struct{}

func (star) build(b *builder) {
	b.write("*")
}
