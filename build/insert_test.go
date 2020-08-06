package build

import "testing"

func TestInsert(t *testing.T) {
	for _, tt := range []struct {
		cmd  *InsertCmd
		out  string
		args []interface{}
	}{{
		cmd: InsertInto("table").
			Values(
				Value("foo", Bind("hello")),
				Value("bar", Bind(1)),
			),
		out:  `INSERT INTO "table" ("foo", "bar") VALUES ($1, $2)`,
		args: []interface{}{"hello", 1},
	}, {
		cmd: InsertInto("table"),
		out: `INSERT INTO "table" DEFAULT VALUES`,
	}, {
		cmd: InsertInto("table").
			Values(
				Value("foo", Bind("hello")),
				Value("bar", Bind(1)),
			).
			Returning(Columns("one", "two", "three")...),
		out:  `INSERT INTO "table" ("foo", "bar") VALUES ($1, $2) RETURNING "one", "two", "three"`,
		args: []interface{}{"hello", 1},
	}, {
		cmd: InsertInto("table").
			Values(
				Value("foo", Bind("hello")),
				Value("bar", Bind(1)),
			).
			OnConflict(DoNothing),
		out:  `INSERT INTO "table" ("foo", "bar") VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		args: []interface{}{"hello", 1},
	}, {
		cmd: InsertInto("table").
			Values(
				Value("foo", Bind("hello")),
				Value("bar", Bind(1)),
			).
			OnConflictTarget("target", DoNothing),
		out:  `INSERT INTO "table" ("foo", "bar") VALUES ($1, $2) ON CONFLICT ("target") DO NOTHING`,
		args: []interface{}{"hello", 1},
	}, {
		cmd: InsertInto("table").
			Values(
				Value("foo", Bind("hello")),
				Value("bar", Bind(1)),
			).
			OnConflict(DoUpdateSet(
				Value("foo", Bind("hello")),
				Value("bar", Bind(1)),
			)),
		out:  `INSERT INTO "table" ("foo", "bar") VALUES ($1, $2) ON CONFLICT DO UPDATE SET "foo" = $3, "bar" = $4`,
		args: []interface{}{"hello", 1, "hello", 1},
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
