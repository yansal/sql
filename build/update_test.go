package build

import "testing"

func TestUpdate(t *testing.T) {
	for _, tt := range []struct {
		cmd  *UpdateCmd
		out  string
		args []interface{}
	}{{
		cmd: Update("table").
			Set(
				Value("foo", Bind("hello")),
				Value("bar", Bind(1)),
			),
		out:  `UPDATE "table" SET "foo" = $1, "bar" = $2`,
		args: []interface{}{"hello", 1},
	}, {
		cmd: Update("table").
			Set(
				Value("foo", Bind("hello")),
				Value("bar", Bind(1)),
			).
			Where(Infix(Ident("foo")).Equal(Bind(0))),
		out:  `UPDATE "table" SET "foo" = $1, "bar" = $2 WHERE "foo" = $3`,
		args: []interface{}{"hello", 1, 0},
	}, {
		cmd: Update("table").
			Set(
				Value("foo", Bind("hello")),
				Value("bar", Bind(1)),
			).Returning(Columns("one", "two", "three")...),
		out:  `UPDATE "table" SET "foo" = $1, "bar" = $2 RETURNING "one", "two", "three"`,
		args: []interface{}{"hello", 1},
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
