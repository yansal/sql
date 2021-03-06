package build

import "testing"

func TestJoin(t *testing.T) {
	for _, tt := range []struct {
		stmt *SelectStmt
		out  string
		args []interface{}
	}{{
		stmt: Select(Columns("t1.foo", "t2.bar")...).
			From(FromItem(Ident("t1")).Join(Ident("t2")).On(Ident("t1.user_id").Equal(Ident("t2.id")))),
		out: `SELECT "t1"."foo", "t2"."bar" FROM "t1" JOIN "t2" ON "t1"."user_id" = "t2"."id"`,
	}, {
		stmt: Select(Columns("t1.foo", "t2.bar")...).
			From(FromItem(Ident("t1")).LeftJoin(Ident("t2")).On(Ident("t1.user_id").Equal(Ident("t2.id")))),
		out: `SELECT "t1"."foo", "t2"."bar" FROM "t1" LEFT JOIN "t2" ON "t1"."user_id" = "t2"."id"`,
	}, {
		stmt: Select(Columns("t1.foo", "t2.bar", "t3.baz")...).
			From(FromItem(Ident("t1")).
				Join(Ident("t2")).On(Ident("t1.id").Equal(Ident("t2.id"))).
				Join(Ident("t3")).On(Ident("t2.id").Equal(Ident("t3.id"))),
			),
		out: `SELECT "t1"."foo", "t2"."bar", "t3"."baz" FROM "t1" JOIN "t2" ON "t1"."id" = "t2"."id" JOIN "t3" ON "t2"."id" = "t3"."id"`,
	}, {
		stmt: Select(Columns("t1.foo", "t2.bar")...).
			From(FromItem(Ident("t1")).Join(Ident("t2")).On(
				CallExpr("date_trunc", String("month"), Ident("t1.foo")).Equal(Ident("t2.bar")),
			)),
		out: `SELECT "t1"."foo", "t2"."bar" FROM "t1" JOIN "t2" ON date_trunc('month', "t1"."foo") = "t2"."bar"`,
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
