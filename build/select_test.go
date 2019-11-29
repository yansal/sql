package build

import "testing"

func TestSelect(t *testing.T) {
	for _, tt := range []struct {
		stmt    *SelectStmt
		command string
		args    []interface{}
	}{{
		stmt: Select("foo").From("bar").
			Where(Expr("foo").In(Bind([]string{"hello", "world"}))),
		command: `SELECT foo FROM bar WHERE foo IN ($1, $2)`,
		args:    []interface{}{"hello", "world"},
	}, {
		stmt:    Select("count(*), foo").From("bar").GroupBy("foo"),
		command: `SELECT count(*), foo FROM bar GROUP BY foo`,
	}, {
		stmt: Select("t1.foo", "t2.bar").
			From(Join("t1", "t2", Expr("t1.user_id").Equal("t2.id"))),
		command: `SELECT t1.foo, t2.bar FROM t1 JOIN t2 ON t1.user_id = t2.id`,
	}, {
		stmt: Select("now").From(
			FromExpr(
				Select("now()"),
			).As("now"),
		),
		command: `SELECT now FROM (SELECT now()) AS now`,
	}, {
		stmt:    Select(ColumnExpr("foo").As("bar")),
		command: `SELECT foo AS bar`,
	}, {
		stmt:    Select("foo").From("bar").OrderBy(OrderExpr(Expr("foo").Equal(Bind("hello")), Desc)),
		command: `SELECT foo FROM bar ORDER BY foo = $1 DESC`,
		args:    []interface{}{"hello"},
	}} {
		command, args := tt.stmt.Build()
		assertf(t, command == tt.command, "expected %q, got %q", tt.command, command)
		assertf(t, len(args) == len(tt.args), "expected %d args, got %d", len(tt.args), len(args))
		minlen := len(args)
		if len(tt.args) < minlen {
			minlen = len(tt.args)
		}
		for i := 0; i < minlen; i++ {
			assertf(t, args[i] == tt.args[i], "expected %#v, got %#v", tt.args[i], args[i])
		}
	}
}

func assertf(t *testing.T, ok bool, msg string, args ...interface{}) {
	t.Helper()
	if !ok {
		t.Errorf(msg, args...)
	}
}
