package build

import (
	"testing"
	"time"
)

func TestSelect(t *testing.T) {
	now := time.Now()
	for _, tt := range []struct {
		cmd  *SelectCmd
		out  string
		args []interface{}
	}{{
		cmd: Select(Columns("foo")...).
			From(Ident("table")).
			Where(Ident("created_at").LessThan(Bind(now))),
		out:  `SELECT "foo" FROM "table" WHERE "created_at" < $1`,
		args: []interface{}{now},
	}, {
		cmd: Select(Columns("foo", "bar")...).From(Ident("table")).
			Where(Ident("foo").In(Bind([]string{"hello", "world"}))),
		out:  `SELECT "foo", "bar" FROM "table" WHERE "foo" IN ($1, $2)`,
		args: []interface{}{"hello", "world"},
	}, {
		cmd: Select(Columns("foo")...).From(Ident("bar")).
			Where(Ident("foo").IsNull()),
		out: `SELECT "foo" FROM "bar" WHERE "foo" IS NULL`,
	}, {
		cmd:  Select(CallExpr("foo", Bind("hello"), Int(123))),
		out:  `SELECT foo($1, 123)`,
		args: []interface{}{"hello"},
	}, {
		cmd: Select(Columns("foo")...).From(Ident("bar")).Limit(Int(1)).Offset(Int(2)),
		out: `SELECT "foo" FROM "bar" LIMIT 1 OFFSET 2`,
	}, {
		cmd:  Select(Columns("foo")...).From(Ident("bar")).Limit(Bind(1)).Offset(Bind(2)),
		out:  `SELECT "foo" FROM "bar" LIMIT $1 OFFSET $2`,
		args: []interface{}{1, 2},
	}, {
		cmd: Select(CallExpr("count", Star), Ident("foo")).From(Ident("bar")).GroupBy(Ident("foo")),
		out: `SELECT count(*), "foo" FROM "bar" GROUP BY "foo"`,
	}, {
		cmd: Select(Columns("now")...).From(
			FromExpr(Select(CallExpr("now"))).As("now")),
		out: `SELECT "now" FROM (SELECT now()) AS "now"`,
	}, {
		cmd: Select(
			ColumnExpr(Ident("foo")).As("bar"),
			ColumnExpr(CallExpr("now")).As("now"),
			ColumnExpr(CallExpr("to_char", Ident("month"), String("YYYY-MM"))).As("month"),
			ColumnExpr(CallExpr("sum", Ident("amount")).Op("/", Int(100))).As("sum_amount"),
		),
		out: `SELECT "foo" AS "bar", now() AS "now", to_char("month", 'YYYY-MM') AS "month", sum("amount") / 100 AS "sum_amount"`,
	}, {
		cmd: Select(Columns("foo")...).From(
			Ident("bar"),
			FromExpr(CallExpr("websearch_to_tsquery", Bind("hello"))).As("tsquery"),
		).OrderBy(
			Order(CallExpr("ts_rank", Ident("tsv"), Ident("tsquery")), Desc),
		),
		out:  `SELECT "foo" FROM "bar", websearch_to_tsquery($1) AS "tsquery" ORDER BY ts_rank("tsv", "tsquery") DESC`,
		args: []interface{}{"hello"},
	}, {
		cmd: Select(Columns("foo")...).From(Ident("bar")).OrderBy(Ident("foo")),
		out: `SELECT "foo" FROM "bar" ORDER BY "foo"`,
	}, {
		cmd: Select(Columns("foo")...).
			From(Ident("bar")).
			OrderBy(Order(Ident("foo").Equal(Bind("hello")), Desc)),
		out:  `SELECT "foo" FROM "bar" ORDER BY "foo" = $1 DESC`,
		args: []interface{}{"hello"},
	}, {
		cmd: Select(Columns("foo")...).
			From(Ident("bar")).
			OrderBy(Order(Ident("foo").Equal(Bind("hello")), Desc).Nulls(Last)),
		out:  `SELECT "foo" FROM "bar" ORDER BY "foo" = $1 DESC NULLS LAST`,
		args: []interface{}{"hello"},
	}, {
		cmd: Select(Columns("location", "time", "report")...).
			DistinctOn(Ident("location")).
			From(Ident("weather_reports")).
			OrderBy(Ident("location"), Order(Ident("time"), Desc)),
		out: `SELECT DISTINCT ON ("location") "location", "time", "report" FROM "weather_reports" ORDER BY "location", "time" DESC`,
	}, {
		cmd: Select(Ident("distributors.name")).
			From(Ident("distributors")).
			Where(Ident("distributors.name").Op("LIKE", String("W%"))).
			Union(
				Select(Ident("actors.name")).
					From(Ident("actors")).
					Where(Ident("actors.name").Op("LIKE", String("W%"))),
			),
		out: `SELECT "distributors"."name" FROM "distributors" WHERE "distributors"."name" LIKE 'W%' UNION SELECT "actors"."name" FROM "actors" WHERE "actors"."name" LIKE 'W%'`,
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
