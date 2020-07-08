package build

import (
	"testing"
)

func TestWindowFunction(t *testing.T) {
	for _, tt := range []struct {
		cmd  *SelectCmd
		out  string
		args []interface{}
	}{{
		cmd: Select(
			Ident("depname"),
			Ident("empno"),
			Ident("salary"),
			WindowFunction("avg", Ident("salary")).Over(PartitionBy(Ident("depname"))),
		).
			From(Ident("empsalary")),
		out: `SELECT "depname", "empno", "salary", avg("salary") OVER (PARTITION BY "depname") FROM "empsalary"`,
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
