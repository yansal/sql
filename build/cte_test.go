package build

import "testing"

func TestCTE(t *testing.T) {
	for _, tt := range []struct {
		stmt *SelectStmt
		out  string
		args []interface{}
	}{{
		stmt: With("z", Select(Int(1))).Select(Star).From(Ident("z")),
		out:  `WITH z AS ( SELECT 1 ) SELECT * FROM "z"`,
	}, {
		stmt: With("z", Select(Int(1))).With("y", Select(Int(2))).Select(Star).From(Ident("z"), Ident("y")),
		out:  `WITH z AS ( SELECT 1 ), y AS ( SELECT 2 ) SELECT * FROM "z", "y"`,
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
