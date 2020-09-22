package build

import "testing"

func TestAggr(t *testing.T) {
	for _, tt := range []struct {
		cmd  *SelectCmd
		out  string
		args []interface{}
	}{{
		cmd: Select(
			ColumnExpr(
				CallExpr("count", Star),
			).As("unfiltered"),
			ColumnExpr(
				Aggr(CallExpr("count", Star)).FilterWhere(Ident("i").LessThan(Int(5))),
			).As("filtered"),
		).
			From(FromExpr(CallExpr("generate_series", Int(1), Int(10))).As("i")),
		out: `SELECT count(*) AS "unfiltered", count(*) FILTER (WHERE "i" < 5) AS "filtered" FROM generate_series(1, 10) AS "i"`,
	}} {
		t.Run(tt.out, func(t *testing.T) {
			out, args := tt.cmd.Build()
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
