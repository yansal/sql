package build

import (
	"fmt"
	"strconv"
	"strings"
)

type Expression interface{ build(*builder) }

// Bind binds a value.
func Bind(value interface{}) *InfixExpr {
	return &InfixExpr{left: &bind{value: value}}
}

type bind struct{ value interface{} }

func (bind *bind) build(b *builder) {
	b.bind(bind.value)
}

// An InfixExpr is an infix expression.
type InfixExpr struct {
	left  Expression
	op    string
	right Expression
}

func (i *InfixExpr) build(b *builder) {
	if i.left != nil {
		i.left.build(b)
	}

	if i.op == "" {
		return
	}
	if i.left != nil {
		b.write(" ")
	}
	b.write(i.op)

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

// Or invokes the OR operator.
func (i *InfixExpr) Or(right Expression) *InfixExpr {
	return i.Op("OR", right)
}

// Equal invokes the = operator.
func (i *InfixExpr) Equal(right Expression) *InfixExpr {
	return i.Op("=", right)
}

// NotEqual invokes the != operator.
func (i *InfixExpr) NotEqual(right Expression) *InfixExpr {
	return i.Op("!=", right)
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

// IsNull adds the IS NULL predicate.
func (i *InfixExpr) IsNull() *InfixExpr {
	return &InfixExpr{left: &InfixExpr{left: i.left, op: "IS NULL"}}
}

// IsNotNull adds the IS NOT NULL predicate.
func (i *InfixExpr) IsNotNull() *InfixExpr {
	return &InfixExpr{left: &InfixExpr{left: i.left, op: "IS NOT NULL"}}
}

func CallExpr(function string, args ...Expression) *InfixExpr {
	return &InfixExpr{left: &callExpr{function: function, args: args}}
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

// Ident returns an expression with the identifier s.
func Ident(s string) *InfixExpr { return &InfixExpr{left: identifier(s)} }

type identifier string

func (i identifier) build(b *builder) {
	split := strings.Split(string(i), ".")
	quoted := make([]string, 0, len(split))
	for i := range split {
		quoted = append(quoted,
			strconv.Quote(split[i]), // TODO: quote only if the identifier must be quoted?
		)
	}
	b.write(strings.Join(quoted, "."))
}

func Bool(b bool) Expression { return boolExpr(b) }

type boolExpr bool

func (be boolExpr) build(b *builder) {
	b.write(strconv.FormatBool(bool(be)))
}

func Int(i int) Expression { return intExpr(i) }

type intExpr int

func (i intExpr) build(b *builder) {
	b.write(fmt.Sprintf("%d", i))
}

func Int64(i int64) Expression { return int64Expr(i) }

type int64Expr int64

func (i int64Expr) build(b *builder) {
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

// Raw returns a raw expression.
func Raw(s string) *InfixExpr { return &InfixExpr{left: raw(s)} }

type raw string

func (r raw) build(b *builder) {
	b.write(string(r))
}

// Not adds the NOT operator to an expression.
func Not(expr Expression) *InfixExpr {
	return &InfixExpr{left: nil, op: "NOT", right: expr}
}

func ParenExpr(expr Expression) *InfixExpr {
	return &InfixExpr{left: &parenExpr{expr: expr}}
}

type parenExpr struct{ expr Expression }

func (p *parenExpr) build(b *builder) {
	b.write("(")
	p.expr.build(b)
	b.write(")")
}
