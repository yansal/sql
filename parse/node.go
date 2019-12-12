package parse

import (
	"fmt"
	"strconv"
)

func Parse(in []byte) (Node, error) {
	tokens, err := tokenize(in)
	if err != nil {
		return nil, err
	}
	return parse(tokens)
}

func parse(tokens []token) (Node, error) {
	p := &parser{tokens: tokens}
	node, err := p.parseSelect()
	if err != nil {
		return nil, err
	}

	if len(p.tokens) != 0 {
		return token{}, fmt.Errorf("unexpected %d remaining token(s), first one is: %s (%s)", len(p.tokens), p.tokens[0].typ, p.tokens[0].value)
	}
	return node, nil
}

type parser struct {
	tokens []token
}

func (p *parser) parseSelect() (selectnode, error) {
	var s selectnode
	if p.peek(tokenselect) {
		p.consume(tokenselect)
		columns, err := p.parseSelectColumns()
		if err != nil {
			return selectnode{}, err
		}
		s.columns = columns
	}

	if p.peek(tokenfrom) {
		p.consume(tokenfrom)
		fromitems, err := p.parseFromItems()
		if err != nil {
			return selectnode{}, err
		}
		s.fromitems = fromitems
	}
	return s, nil
}

func (p *parser) parseSelectColumns() ([]columnnode, error) {
	var nodes []columnnode

	node, err := p.parseSelectColumn()
	if err != nil {
		return nil, err
	}
	nodes = append(nodes, node)

	for p.peek(tokencomma) {
		if _, err := p.consume(tokencomma); err != nil {
			return nil, err
		}
		node, err := p.parseSelectColumn()
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

func (p *parser) parseSelectColumn() (columnnode, error) {
	expr, err := p.parseExpr()
	if err != nil {
		return columnnode{}, err
	}
	col := columnnode{node: expr}

	if p.peek(tokenas) {
		p.consume(tokenas)
		ident, err := p.consume(tokenquotedident)
		if err != nil {
			return col, err
		}
		col.as = string(ident.value)
	}

	return col, nil
}

func (p *parser) parseFromItems() ([]fromitemnode, error) {
	var nodes []fromitemnode

	node, err := p.parseFromItem()
	if err != nil {
		return nil, err
	}
	nodes = append(nodes, node)

	for p.peek(tokencomma) {
		if _, err := p.consume(tokencomma); err != nil {
			return nil, err
		}
		node, err := p.parseFromItem()
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, node)
	}
	return nodes, nil
}

func (p *parser) parseFromItem() (fromitemnode, error) {
	expr, err := p.parseExpr()
	if err != nil {
		return fromitemnode{}, err
	}
	col := fromitemnode{node: expr}

	if p.peek(tokenas) {
		p.consume(tokenas)
		ident, err := p.consume(tokenquotedident)
		if err != nil {
			return col, err
		}
		col.as = string(ident.value)
	}

	return col, nil
}

func (p *parser) parseExpr() (Node, error) {
	var node Node
	switch {
	case p.peek(tokennumber):
		n, _ := p.consume(tokennumber)
		value, err := strconv.Atoi(string(n.value))
		if err != nil {
			return nil, err
		}
		node = numbernode{value: value}
	case p.peek(tokenstring):
		s, _ := p.consume(tokenstring)
		node = stringnode{value: string(s.value)} // TODO: remove single quotes
	case p.peek(tokenident) && p.peekN(tokenoparen, 1):
		var err error
		node, err = p.parseCall()
		if err != nil {
			return nil, err
		}
	case p.peek(tokenident):
		t, _ := p.consume(tokenident)
		node = identnode{string(t.value)}
	case p.peek(tokenquotedident):
		t, _ := p.consume(tokenquotedident)
		node = identnode{string(t.value)}
	case p.peek(tokenstar):
		s, _ := p.consume(tokenstar)
		node = identnode{name: string(s.value)}
	default:
		if len(p.tokens) == 0 {
			return token{}, fmt.Errorf("expected expression, got eof")
		}
		return nil, fmt.Errorf("expected expression, got %s (%s)", p.tokens[0].typ, p.tokens[0].value)
	}
	// TODO: parse op
	if p.peek(tokenop) {
		left := node
		op, _ := p.consume(tokenop)
		right, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		node = infixnode{left: left, op: string(op.value), right: right}
	}
	return node, nil
}

func (p *parser) parseCall() (callnode, error) {
	name, err := p.consume(tokenident)
	if err != nil {
		return callnode{}, err
	}
	if _, err := p.consume(tokenoparen); err != nil {
		return callnode{}, err
	}

	node := callnode{name: string(name.value)}

	if !p.peek(tokencparen) {
		arg, err := p.parseExpr()
		if err != nil {
			return node, err
		}
		node.args = append(node.args, arg)
	}
	for p.peek(tokencomma) {
		p.consume(tokencomma)
		arg, err := p.parseExpr()
		if err != nil {
			return node, err
		}
		node.args = append(node.args, arg)
	}

	if _, err := p.consume(tokencparen); err != nil {
		return node, err
	}
	return node, nil
}

func (p *parser) consume(expected tokentype) (token, error) {
	if len(p.tokens) == 0 {
		return token{}, fmt.Errorf("expected token type %s, got eof", expected)
	}
	token := p.tokens[0]
	if token.typ != expected {
		return token, fmt.Errorf("expected token type %s, got %s", expected, token.typ)
	}
	p.tokens = p.tokens[1:]
	return token, nil
}

func (p *parser) peek(expected tokentype) bool {
	return p.peekN(expected, 0)
}

func (p *parser) peekN(expected tokentype, n int) bool {
	if len(p.tokens) <= n {
		return false
	}
	return p.tokens[n].typ == expected
}

type Node interface{}

type selectnode struct {
	columns   []columnnode
	fromitems []fromitemnode
}

type columnnode struct {
	node Node
	as   string
}

type fromitemnode struct {
	node Node
	as   string
}

type identnode struct {
	name string
	// TODO: add bool quoted?
}

type callnode struct {
	name string
	args []Node
}

type infixnode struct {
	left  Node
	op    string
	right Node
}

type numbernode struct {
	value int
}

type stringnode struct {
	value string
}
