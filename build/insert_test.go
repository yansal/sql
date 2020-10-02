package build

import "testing"

func TestInsert(t *testing.T) {
	for _, tt := range []struct {
		stmt *InsertStmt
		out  string
		args []interface{}
	}{{
		stmt: InsertInto("table").DefaultValues(),
		out:  `INSERT INTO "table" DEFAULT VALUES`,
	}, {
		stmt: InsertInto("table").
			Values(Bind("hello"), Bind(1)),
		out:  `INSERT INTO "table" VALUES ($1, $2)`,
		args: []interface{}{"hello", 1},
	}, {
		stmt: InsertInto("table", "foo", "bar").
			Values(Bind("hello"), Bind(1)),
		out:  `INSERT INTO "table" ("foo", "bar") VALUES ($1, $2)`,
		args: []interface{}{"hello", 1},
	}, {
		stmt: InsertInto("table").
			Query(Select(Ident("foo")).
				From(Ident("bar")).
				Where(Ident("baz").Equal(Bind(1))),
			),
		out:  `INSERT INTO "table" SELECT "foo" FROM "bar" WHERE "baz" = $1`,
		args: []interface{}{1},
	}, {
		stmt: InsertInto("table", "foo", "bar").
			Values(Bind("hello"), Bind(1)).
			Returning(Columns("one", "two", "three")...),
		out:  `INSERT INTO "table" ("foo", "bar") VALUES ($1, $2) RETURNING "one", "two", "three"`,
		args: []interface{}{"hello", 1},
	}, {
		stmt: InsertInto("table", "foo", "bar").
			Values(Bind("hello"), Bind(1)).
			OnConflict(DoNothing),
		out:  `INSERT INTO "table" ("foo", "bar") VALUES ($1, $2) ON CONFLICT DO NOTHING`,
		args: []interface{}{"hello", 1},
	}, {
		stmt: InsertInto("table", "foo", "bar").
			Values(Bind("hello"), Bind(1)).
			OnConflictTarget("target", DoNothing),
		out:  `INSERT INTO "table" ("foo", "bar") VALUES ($1, $2) ON CONFLICT ("target") DO NOTHING`,
		args: []interface{}{"hello", 1},
	}, {
		stmt: InsertInto("table", "foo", "bar").
			Values(Bind("hello"), Bind(1)).
			OnConflict(DoUpdateSet(
				Assign("foo", Bind("hello")),
				Assign("bar", Bind(1)),
			)),
		out:  `INSERT INTO "table" ("foo", "bar") VALUES ($1, $2) ON CONFLICT DO UPDATE SET "foo" = $3, "bar" = $4`,
		args: []interface{}{"hello", 1, "hello", 1},
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
