//go:generate stringer -type=tokentype

package parse

import (
	"bytes"
	"fmt"
	"regexp"
)

type tokentype int

const (
	tokenselect tokentype = iota
	tokenas
	tokenfrom
	tokenjoin
	tokenon
	tokenwhere
	tokengroupby
	tokenorderby
	tokenlimit
	tokenoffset
	tokenasc
	tokendesc
	tokenand
	tokenin
	tokenisnull
	tokencomma
	tokenoparen
	tokencparen
	tokenop
	tokenstar
	tokenident
	tokenquotedident
	tokennumber
	tokenstring
	tokenbindvalue
)

var tokentypes = []struct {
	tokentype tokentype
	regexp    *regexp.Regexp
}{
	// keywords
	{tokenselect, regexp.MustCompile(`(?i)\A(\bSELECT\b)`)},
	{tokenas, regexp.MustCompile(`(?i)\A(\bAS\b)`)},
	{tokenfrom, regexp.MustCompile(`(?i)\A(\bFROM\b)`)},
	{tokenjoin, regexp.MustCompile(`(?i)\A(\bJOIN\b)`)},
	{tokenon, regexp.MustCompile(`(?i)\A(\bON\b)`)},
	{tokenwhere, regexp.MustCompile(`(?i)\A(\bWHERE\b)`)},
	{tokengroupby, regexp.MustCompile(`(?i)\A(\bGROUP BY\b)`)},
	{tokenorderby, regexp.MustCompile(`(?i)\A(\bORDER BY\b)`)},
	{tokenlimit, regexp.MustCompile(`(?i)\A(\bLIMIT\b)`)},
	{tokenoffset, regexp.MustCompile(`(?i)\A(\bOFFSET\b)`)},
	{tokenasc, regexp.MustCompile(`(?i)\A(\bASC\b)`)},
	{tokendesc, regexp.MustCompile(`(?i)\A(\bDESC\b)`)},
	{tokenand, regexp.MustCompile(`(?i)\A(\bAND\b)`)},
	{tokenin, regexp.MustCompile(`(?i)\A(\bIN\b)`)},
	{tokenisnull, regexp.MustCompile(`(?i)\A(\bIS NULL\b)`)},

	// punctuation, operators
	{tokencomma, regexp.MustCompile(`\A(\,)`)},
	{tokenoparen, regexp.MustCompile(`\A(\()`)},
	{tokencparen, regexp.MustCompile(`\A(\))`)},
	{tokenop, regexp.MustCompile(`\A(=|\+)`)},
	{tokenstar, regexp.MustCompile(`\A(\*)`)},

	// literals
	{tokennumber, regexp.MustCompile(`\A(\d+)`)},
	{tokenstring, regexp.MustCompile(`\A('\w+')`)},
	{tokenbindvalue, regexp.MustCompile(`\A(\$\d+)`)},

	// identifiers
	{tokenquotedident, regexp.MustCompile(`\A(\"[a-zA-Z_]+\")`)},
	{tokenident, regexp.MustCompile(`\A(\*|\b[a-zA-Z_]+\b)`)},
}

type token struct {
	typ   tokentype
	value []byte
}

func tokenizeOne(in []byte) (token, error) {
	for _, tt := range tokentypes {
		submatches := tt.regexp.FindSubmatch(in)
		if len(submatches) == 2 {
			return token{
				typ:   tt.tokentype,
				value: submatches[1],
			}, nil
		}
	}
	return token{}, fmt.Errorf("couldn't tokenize %q", in)
}

func tokenize(in []byte) ([]token, error) {
	var tokens []token
	for len(in) > 0 {
		t, err := tokenizeOne(in)
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, t)
		in = in[len(t.value):]
		in = bytes.TrimSpace(in)
	}
	return tokens, nil
}
