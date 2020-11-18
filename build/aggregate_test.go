package build

import "testing"

func TestAggr(t *testing.T) {
	for _, tt := range []struct {
		stmt *SelectStmt
		out  string
		args []interface{}
	}{{
		stmt: Select(
			ColumnExpr(
				CallExpr("count", Star),
			).As("unfiltered"),
			ColumnExpr(
				Aggr("count", Star).FilterWhere(Ident("i").LessThan(Int(5))),
			).As("filtered"),
		).
			From(FromExpr(CallExpr("generate_series", Int(1), Int(10))).As("i")),
		out: `SELECT count(*) AS "unfiltered", count(*) FILTER (WHERE "i" < 5) AS "filtered" FROM generate_series(1, 10) AS "i"`,
	}, {
		stmt: Select(AggrDistinct("count", Ident("foo"))).
			From(Ident("bar")),
		out: `SELECT count(DISTINCT "foo") FROM "bar"`,
	}, {
		stmt: Select(Aggr("array_agg", Ident("a")).OrderBy(Order(Ident("b"), Desc))).
			From(Ident("table")),
		out: `SELECT array_agg("a" ORDER BY "b" DESC) FROM "table"`,
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
