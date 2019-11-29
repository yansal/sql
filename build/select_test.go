package build

import "testing"

func TestSelect(t *testing.T) {
	for _, tt := range []struct {
		cmd  *SelectCmd
		out  string
		args []interface{}
	}{{
		cmd: Select("foo").From("bar").
			Where(Expr("foo").In(Bind([]string{"hello", "world"}))),
		out:  `SELECT foo FROM bar WHERE foo IN ($1, $2)`,
		args: []interface{}{"hello", "world"},
	}, {
		cmd: Select("foo").From("bar").
			Where(Expr("foo").IsNull()),
		out: `SELECT foo FROM bar WHERE foo IS NULL`,
	}, {
		cmd:  Select(CallExpr("foo", Bind("hello"), 123)),
		out:  `SELECT foo($1, 123)`,
		args: []interface{}{"hello"},
	}, {
		cmd: Select("count(*)", "foo").From("bar").Limit(1).Offset(2),
		out: `SELECT count(*), foo FROM bar LIMIT 1 OFFSET 2`,
	}, {
		cmd:  Select("count(*)", "foo").From("bar").Limit(Bind(1)).Offset(Bind(2)),
		out:  `SELECT count(*), foo FROM bar LIMIT $1 OFFSET $2`,
		args: []interface{}{1, 2},
	}, {
		cmd: Select("count(*)", "foo").From("bar").GroupBy("foo"),
		out: `SELECT count(*), foo FROM bar GROUP BY foo`,
	}, {
		cmd: Select("t1.foo", "t2.bar").
			From(Join("t1", "t2", Expr("t1.user_id").Equal("t2.id"))),
		out: `SELECT t1.foo, t2.bar FROM t1 JOIN t2 ON t1.user_id = t2.id`,
	}, {
		cmd: Select("now").From(
			FromExpr(
				Select("now()"),
			).As("now"),
		),
		out: `SELECT now FROM (SELECT now()) AS now`,
	}, {
		cmd: Select(ColumnExpr("foo").As("bar")),
		out: `SELECT foo AS bar`,
	}, {
		cmd:  Select("foo").From("bar").OrderBy(OrderExpr(Expr("foo").Equal(Bind("hello")), Desc)),
		out:  `SELECT foo FROM bar ORDER BY foo = $1 DESC`,
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
