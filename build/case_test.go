package build

import "testing"

func TestCase(t *testing.T) {
	for _, tt := range []struct {
		stmt *SelectStmt
		out  string
		args []interface{}
	}{{
		stmt: Select(
			Ident("a"),
			CaseWhen(Ident("a").Equal(Int(1)), String("one")).When(Ident("a").Equal(Int(2)), String("two")).Else(String("other")),
		).
			From(Ident("test")),
		out: `SELECT "a", CASE WHEN "a" = 1 THEN 'one' WHEN "a" = 2 THEN 'two' ELSE 'other' END FROM "test"`,
	}} {
		t.Run(tt.out, func(t *testing.T) {
			out, args := tt.stmt.Build()
			assertf(t, out == tt.out, "expected %q, got %q", tt.out, out)
			assertf(t, len(args) == len(tt.args), "expected %d args, got %d", len(tt.args), len(args))
			minlen := len(args)
			if len(tt.args) < minlen {
				minlen = len(tt.args)
			}
			for i := 0; i < minlen; i++ {
				assertf(t, args[i] == tt.args[i], "expected %#v, got %#v", tt.args[i], args[i])
			}
		})
	}
}
