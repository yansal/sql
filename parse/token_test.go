package parse

import "testing"

func TestTokenize(t *testing.T) {
	for _, tt := range []struct {
		in  []byte
		out []token
	}{
		{in: []byte(`SELECT "foo"`), out: []token{
			{tokentype: tokenselect, value: []byte("SELECT")},
			{tokentype: tokenquotedident, value: []byte(`"foo"`)},
		}},
		{in: []byte(`SELECT foo`), out: []token{
			{tokentype: tokenselect, value: []byte("SELECT")},
			{tokentype: tokenident, value: []byte("foo")},
		}},
		{in: []byte(`select "foo"`), out: []token{
			{tokentype: tokenselect, value: []byte("select")},
			{tokentype: tokenquotedident, value: []byte(`"foo"`)},
		}},
		{in: []byte(`SELECT "foo", "bar"`), out: []token{
			{tokentype: tokenselect, value: []byte("SELECT")},
			{tokentype: tokenquotedident, value: []byte(`"foo"`)},
			{tokentype: tokencomma, value: []byte(`,`)},
			{tokentype: tokenquotedident, value: []byte(`"bar"`)},
		}},
		{in: []byte(`SELECT "foo" FROM "bar"`), out: []token{
			{tokentype: tokenselect, value: []byte("SELECT")},
			{tokentype: tokenquotedident, value: []byte(`"foo"`)},
			{tokentype: tokenfrom, value: []byte("FROM")},
			{tokentype: tokenquotedident, value: []byte(`"bar"`)},
		}},
		{in: []byte(`SELECT "foo" FROM "bar" WHERE "foo" = 0`), out: []token{
			{tokentype: tokenselect, value: []byte("SELECT")},
			{tokentype: tokenquotedident, value: []byte(`"foo"`)},
			{tokentype: tokenfrom, value: []byte("FROM")},
			{tokentype: tokenquotedident, value: []byte(`"bar"`)},
			{tokentype: tokenwhere, value: []byte("WHERE")},
			{tokentype: tokenquotedident, value: []byte(`"foo"`)},
			{tokentype: tokeneq, value: []byte("=")},
			{tokentype: tokenint, value: []byte(`0`)},
		}},
		{in: []byte(`SELECT "foo" FROM "bar" WHERE "foo" IN (1, 2)`), out: []token{
			{tokentype: tokenselect, value: []byte("SELECT")},
			{tokentype: tokenquotedident, value: []byte(`"foo"`)},
			{tokentype: tokenfrom, value: []byte("FROM")},
			{tokentype: tokenquotedident, value: []byte(`"bar"`)},
			{tokentype: tokenwhere, value: []byte("WHERE")},
			{tokentype: tokenquotedident, value: []byte(`"foo"`)},
			{tokentype: tokenin, value: []byte("IN")},
			{tokentype: tokenoparen, value: []byte("(")},
			{tokentype: tokenint, value: []byte(`1`)},
			{tokentype: tokencomma, value: []byte(`,`)},
			{tokentype: tokenint, value: []byte(`2`)},
			{tokentype: tokencparen, value: []byte(")")},
		}},
		{in: []byte(`SELECT "foo" FROM "bar" LIMIT $1`), out: []token{
			{tokentype: tokenselect, value: []byte("SELECT")},
			{tokentype: tokenquotedident, value: []byte(`"foo"`)},
			{tokentype: tokenfrom, value: []byte("FROM")},
			{tokentype: tokenquotedident, value: []byte(`"bar"`)},
			{tokentype: tokenlimit, value: []byte("LIMIT")},
			{tokentype: tokenbindvalue, value: []byte(`$1`)},
		}},
		{in: []byte(`SELECT count(*) FROM "bar"`), out: []token{
			{tokentype: tokenselect, value: []byte("SELECT")},
			{tokentype: tokenident, value: []byte(`count`)},
			{tokentype: tokenoparen, value: []byte("(")},
			{tokentype: tokenstar, value: []byte("*")},
			{tokentype: tokencparen, value: []byte(")")},
			{tokentype: tokenfrom, value: []byte("FROM")},
			{tokentype: tokenquotedident, value: []byte(`"bar"`)},
		}},
	} {
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
			assertf(t, tokens[i].tokentype == tt.out[i].tokentype, "expected tokentype %d, got %d", tt.out[i].tokentype, tokens[i].tokentype)
			assertf(t, string(tt.out[i].value) == string(tokens[i].value), "expected value %q, got %q", tt.out[i].value, tokens[i].value)
		}
	}
}

func assertf(t *testing.T, ok bool, msg string, args ...interface{}) {
	t.Helper()
	if !ok {
		t.Errorf(msg, args...)
	}
}
