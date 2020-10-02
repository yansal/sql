package build

import (
	"testing"
)

func TestWindowFunction(t *testing.T) {
	for _, tt := range []struct {
		stmt *SelectStmt
		out  string
		args []interface{}
	}{{
		stmt: Select(
			Ident("depname"),
			Ident("empno"),
			Ident("salary"),
			WindowFunction("avg", Ident("salary")).Over(PartitionBy(Ident("depname"))),
		).
			From(Ident("empsalary")),
		out: `SELECT "depname", "empno", "salary", avg("salary") OVER ( PARTITION BY "depname" ) FROM "empsalary"`,
	}, {
		stmt: Select(
			Ident("depname"),
			Ident("empno"),
			Ident("salary"),
			WindowFunction("rank").Over(PartitionBy(Ident("depname")).OrderBy(Order(Ident("salary"), Desc))),
		).
			From(Ident("empsalary")),
		out: `SELECT "depname", "empno", "salary", rank() OVER ( PARTITION BY "depname" ORDER BY "salary" DESC ) FROM "empsalary"`,
	}, {
		stmt: Select(
			Ident("salary"),
			WindowFunction("sum", Ident("salary")),
		).
			From(Ident("empsalary")),
		out: `SELECT "salary", sum("salary") OVER () FROM "empsalary"`,
	}, {
		stmt: Select(
			Ident("salary"),
			WindowFunction("sum", Ident("salary")).Over(OrderBy(Ident("salary"))),
		).
			From(Ident("empsalary")),
		out: `SELECT "salary", sum("salary") OVER ( ORDER BY "salary" ) FROM "empsalary"`,
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
