package parse

import "fmt"

func Parse(in []byte) (Node, error) {
	tokens, err := tokenize(in)
	if err != nil {
		return nil, err
	}
	return parse(tokens)
}

type Node interface{}

func parse(tokens []token) (Node, error) {
	p := &parser{tokens: tokens}
	return p.parseSelect()
}

type parser struct {
	tokens []token
}

func (p parser) parseSelect() (selectNode, error) {
	if _, err := p.consume(tokenselect); err != nil {
		return selectNode{}, err
	}

	columns, err := p.parseSelectColumns()
	if err != nil {
		return selectNode{}, err
	}
	return selectNode{columns: columns}, nil
}

func (p parser) parseSelectColumns() ([]selectExprNode, error) {
	var nodes []selectExprNode
	token, err := p.consume(tokenquotedident)
	if err != nil {
		return nil, err
	}
	nodes = append(nodes, selectExprNode{ident: string(token.value)})
	for p.peek(tokencomma) {
		if _, err := p.consume(tokencomma); err != nil {
			return nil, err
		}
		token, err := p.consume(tokenquotedident)
		if err != nil {
			return nil, err
		}
		nodes = append(nodes, selectExprNode{ident: string(token.value)})
	}
	return nodes, nil
}

func (p *parser) consume(expected tokentype) (token, error) {
	t := p.tokens[0]
	if t.tokentype != expected {
		return token{}, fmt.Errorf("expected token type %d, got %d", expected, t.tokentype)
	}
	p.tokens = p.tokens[1:]
	return t, nil
}

func (p *parser) peek(expected tokentype) bool {
	if len(p.tokens) == 0 {
		return false
	}
	return p.tokens[0].tokentype == expected
}

type selectNode struct {
	columns []selectExprNode
}

type selectExprNode struct {
	ident string
}
