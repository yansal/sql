package build

import "testing"

func TestUpdate(t *testing.T) {
	for _, tt := range []struct {
		stmt *UpdateStmt
		out  string
		args []interface{}
	}{{
		stmt: Update("table").
			Set(
				Assign("foo", Bind("hello")),
				Assign("bar", Bind(1)),
			),
		out:  `UPDATE "table" SET "foo" = $1, "bar" = $2`,
		args: []interface{}{"hello", 1},
	}, {
		stmt: Update("table").
			Set(
				Assign("foo", Bind("hello")),
				Assign("bar", Bind(1)),
			).
			Where(Ident("foo").Equal(Bind(0))),
		out:  `UPDATE "table" SET "foo" = $1, "bar" = $2 WHERE "foo" = $3`,
		args: []interface{}{"hello", 1, 0},
	}, {
		stmt: Update("table").
			Set(
				Assign("foo", Bind("hello")),
				Assign("bar", Bind(1)),
			).Returning(Columns("one", "two", "three")...),
		out:  `UPDATE "table" SET "foo" = $1, "bar" = $2 RETURNING "one", "two", "three"`,
		args: []interface{}{"hello", 1},
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
