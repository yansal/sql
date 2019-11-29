package build

import "testing"

func TestSelect(t *testing.T) {
	for _, tt := range []struct {
		cmd  *SelectCmd
		out  string
		args []interface{}
	}{{
		cmd: Select(Columns("foo", "bar")...).From(Identifier("table")).
			Where(Infix(Identifier("foo")).In(Bind([]string{"hello", "world"}))),
		out:  `SELECT "foo", "bar" FROM "table" WHERE "foo" IN ($1, $2)`,
		args: []interface{}{"hello", "world"},
	}, {
		cmd: Select(Columns("foo")...).From(Identifier("bar")).
			Where(Infix(Identifier("foo")).IsNull()),
		out: `SELECT "foo" FROM "bar" WHERE "foo" IS NULL`,
	}, {
		cmd:  Select(CallExpr("foo", Bind("hello"), Int(123))),
		out:  `SELECT foo($1, 123)`,
		args: []interface{}{"hello"},
	}, {
		cmd: Select(Columns("foo")...).From(Identifier("bar")).Limit(Int(1)).Offset(Int(2)),
		out: `SELECT "foo" FROM "bar" LIMIT 1 OFFSET 2`,
	}, {
		cmd:  Select(Columns("foo")...).From(Identifier("bar")).Limit(Bind(1)).Offset(Bind(2)),
		out:  `SELECT "foo" FROM "bar" LIMIT $1 OFFSET $2`,
		args: []interface{}{1, 2},
	}, {
		cmd: Select(CallExpr("count", Star), Identifier("foo")).From(Identifier("bar")).GroupBy(Identifier("foo")),
		out: `SELECT count(*), "foo" FROM "bar" GROUP BY "foo"`,
	}, {
		cmd: Select(Columns("t1.foo", "t2.bar")...).
			From(Join(
				Identifier("t1"),
				Identifier("t2"),
				Infix(Identifier("t1.user_id")).Equal(Identifier("t2.id")),
			)),
		out: `SELECT "t1.foo", "t2.bar" FROM "t1" JOIN "t2" ON "t1.user_id" = "t2.id"`,
	}, {
		cmd: Select(Columns("t1.foo", "t2.bar")...).
			From(Join(
				Identifier("t1"),
				Identifier("t2"),
				Infix(CallExpr("date_trunc", String("month"), Identifier("t1.foo"))).Equal(Identifier("t2.bar")),
			)),
		out: `SELECT "t1.foo", "t2.bar" FROM "t1" JOIN "t2" ON date_trunc('month', "t1.foo") = "t2.bar"`,
	}, {
		cmd: Select(Columns("now")...).From(
			FromExpr(Select(CallExpr("now"))).As("now")),
		out: `SELECT "now" FROM (SELECT now()) AS "now"`,
	}, {
		cmd: Select(
			ColumnExpr(Identifier("foo")).As("bar"),
			ColumnExpr(CallExpr("now")).As("now"),
			ColumnExpr(CallExpr("to_char", Identifier("month"), String("YYYY-MM"))).As("month"),
			ColumnExpr(Infix(CallExpr("sum", Identifier("amount"))).Op("/", Int(100))).As("sum_amount"),
		),
		out: `SELECT "foo" AS "bar", now() AS "now", to_char("month", 'YYYY-MM') AS "month", sum("amount") / 100 AS "sum_amount"`,
	}, {
		cmd: Select(Columns("foo")...).From(
			Identifier("bar"),
			FromExpr(CallExpr("websearch_to_tsquery", Bind("hello"))).As("tsquery"),
		).OrderBy(
			OrderExpr(CallExpr("ts_rank", Identifier("tsv"), Identifier("tsquery")), Desc),
		),
		out:  `SELECT "foo" FROM "bar", websearch_to_tsquery($1) AS "tsquery" ORDER BY ts_rank("tsv", "tsquery") DESC`,
		args: []interface{}{"hello"},
	}, {
		cmd: Select(Columns("foo")...).From(Identifier("bar")).OrderBy(Identifier("foo")),
		out: `SELECT "foo" FROM "bar" ORDER BY "foo"`,
	}, {
		cmd:  Select(Columns("foo")...).From(Identifier("bar")).OrderBy(OrderExpr(Infix(Identifier("foo")).Equal(Bind("hello")), Desc)),
		out:  `SELECT "foo" FROM "bar" ORDER BY "foo" = $1 DESC`,
		args: []interface{}{"hello"},
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

func assertf(t *testing.T, ok bool, msg string, args ...interface{}) {
	t.Helper()
	if !ok {
		t.Errorf(msg, args...)
	}
}
