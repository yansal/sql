package parse

import "testing"

func TestTokenize(t *testing.T) {
	for _, tt := range []struct {
		in  []byte
		out []token
	}{
		{in: []byte(`SELECT "foo"`), out: []token{
			{typ: tokenselect, value: []byte("SELECT")},
			{typ: tokenquotedident, value: []byte(`"foo"`)},
		}},
		{in: []byte(`SELECT foo`), out: []token{
			{typ: tokenselect, value: []byte("SELECT")},
			{typ: tokenident, value: []byte("foo")},
		}},
		{in: []byte(`select "foo"`), out: []token{
			{typ: tokenselect, value: []byte("select")},
			{typ: tokenquotedident, value: []byte(`"foo"`)},
		}},
		{in: []byte(`SELECT "foo", "bar"`), out: []token{
			{typ: tokenselect, value: []byte("SELECT")},
			{typ: tokenquotedident, value: []byte(`"foo"`)},
			{typ: tokencomma, value: []byte(`,`)},
			{typ: tokenquotedident, value: []byte(`"bar"`)},
		}},
		{in: []byte(`SELECT "foo" AS "bar"`), out: []token{
			{typ: tokenselect, value: []byte("SELECT")},
			{typ: tokenquotedident, value: []byte(`"foo"`)},
			{typ: tokenas, value: []byte(`AS`)},
			{typ: tokenquotedident, value: []byte(`"bar"`)},
		}},
		{in: []byte(`SELECT f(1, 'string', foo)`), out: []token{
			{typ: tokenselect, value: []byte("SELECT")},
			{typ: tokenident, value: []byte(`f`)},
			{typ: tokenoparen, value: []byte("(")},
			{typ: tokennumber, value: []byte(`1`)},
			{typ: tokencomma, value: []byte(",")},
			{typ: tokenstring, value: []byte(`'string'`)},
			{typ: tokencomma, value: []byte(",")},
			{typ: tokenident, value: []byte(`foo`)},
			{typ: tokencparen, value: []byte(")")},
		}},
		{in: []byte(`SELECT 1+1`), out: []token{
			{typ: tokenselect, value: []byte("SELECT")},
			{typ: tokennumber, value: []byte(`1`)},
			{typ: tokenop, value: []byte("+")},
			{typ: tokennumber, value: []byte(`1`)},
		}},
		{in: []byte(`SELECT "foo" FROM "bar"`), out: []token{
			{typ: tokenselect, value: []byte("SELECT")},
			{typ: tokenquotedident, value: []byte(`"foo"`)},
			{typ: tokenfrom, value: []byte("FROM")},
			{typ: tokenquotedident, value: []byte(`"bar"`)},
		}},
		{in: []byte(`SELECT "foo" FROM "bar" WHERE "foo" = 0`), out: []token{
			{typ: tokenselect, value: []byte("SELECT")},
			{typ: tokenquotedident, value: []byte(`"foo"`)},
			{typ: tokenfrom, value: []byte("FROM")},
			{typ: tokenquotedident, value: []byte(`"bar"`)},
			{typ: tokenwhere, value: []byte("WHERE")},
			{typ: tokenquotedident, value: []byte(`"foo"`)},
			{typ: tokenop, value: []byte("=")},
			{typ: tokennumber, value: []byte(`0`)},
		}},
		{in: []byte(`SELECT "foo" FROM "bar" WHERE "foo" IN (1, 2)`), out: []token{
			{typ: tokenselect, value: []byte("SELECT")},
			{typ: tokenquotedident, value: []byte(`"foo"`)},
			{typ: tokenfrom, value: []byte("FROM")},
			{typ: tokenquotedident, value: []byte(`"bar"`)},
			{typ: tokenwhere, value: []byte("WHERE")},
			{typ: tokenquotedident, value: []byte(`"foo"`)},
			{typ: tokenin, value: []byte("IN")},
			{typ: tokenoparen, value: []byte("(")},
			{typ: tokennumber, value: []byte(`1`)},
			{typ: tokencomma, value: []byte(`,`)},
			{typ: tokennumber, value: []byte(`2`)},
			{typ: tokencparen, value: []byte(")")},
		}},
		{in: []byte(`SELECT "foo" FROM "bar" LIMIT $1`), out: []token{
			{typ: tokenselect, value: []byte("SELECT")},
			{typ: tokenquotedident, value: []byte(`"foo"`)},
			{typ: tokenfrom, value: []byte("FROM")},
			{typ: tokenquotedident, value: []byte(`"bar"`)},
			{typ: tokenlimit, value: []byte("LIMIT")},
			{typ: tokenbindvalue, value: []byte(`$1`)},
		}},
		{in: []byte(`SELECT count(*) FROM "bar"`), out: []token{
			{typ: tokenselect, value: []byte("SELECT")},
			{typ: tokenident, value: []byte(`count`)},
			{typ: tokenoparen, value: []byte("(")},
			{typ: tokenstar, value: []byte("*")},
			{typ: tokencparen, value: []byte(")")},
			{typ: tokenfrom, value: []byte("FROM")},
			{typ: tokenquotedident, value: []byte(`"bar"`)},
		}},
		{in: []byte(`SELECT under_score`), out: []token{
			{typ: tokenselect, value: []byte("SELECT")},
			{typ: tokenident, value: []byte(`under_score`)},
		}},
	} {
		t.Run(string(tt.in), func(t *testing.T) {
			tokens, err := tokenize(tt.in)
			if err != nil {
				t.Fatal(err)
			}
			assertf(t, len(tokens) == len(tt.out), "expected %d tokens, got %d", len(tt.out), len(tokens))
			minlen := len(tokens)
			if len(tt.out) < len(tokens) {
				minlen = len(tt.out)
			}
			for i := 0; i < minlen; i++ {
				assertf(t, tokens[i].typ == tt.out[i].typ, "expected type %s, got %s", tt.out[i].typ, tokens[i].typ)
				assertf(t, string(tt.out[i].value) == string(tokens[i].value), "expected value %q, got %q", tt.out[i].value, tokens[i].value)
			}
		})
	}
}

func assertf(t *testing.T, ok bool, msg string, args ...interface{}) {
	t.Helper()
	if !ok {
		t.Errorf(msg, args...)
	}
}
