package parse

import (
	"reflect"
	"testing"
)

func TestParse(t *testing.T) {
	for _, tt := range []struct {
		in  []byte
		out Node
	}{
		{in: []byte(`SELECT 1`), out: selectnode{columns: []columnnode{
			{node: numbernode{value: 1}},
		}}},
		{in: []byte(`SELECT 1+"foo"`), out: selectnode{columns: []columnnode{
			{node: infixnode{
				left:  numbernode{value: 1},
				op:    "+",
				right: identnode{name: `"foo"`},
			}},
		}}},
		{in: []byte(`SELECT "foo", bar`), out: selectnode{columns: []columnnode{
			{node: identnode{name: `"foo"`}},
			{node: identnode{name: `bar`}},
		}}},
		{in: []byte(`SELECT "foo" AS "bar"`), out: selectnode{columns: []columnnode{
			{node: identnode{name: `"foo"`}, as: `"bar"`},
		}}},
		{in: []byte(`SELECT now()`), out: selectnode{columns: []columnnode{
			{node: callnode{name: "now"}},
		}}},
		{in: []byte(`SELECT count(*)`), out: selectnode{columns: []columnnode{
			{node: callnode{name: "count", args: []Node{identnode{name: "*"}}}},
		}}},
		{in: []byte(`SELECT sum("foo")`), out: selectnode{columns: []columnnode{
			{node: callnode{name: "sum", args: []Node{identnode{name: `"foo"`}}}},
		}}},
		{in: []byte(`SELECT f(1, 'string', foo)`), out: selectnode{columns: []columnnode{
			{node: callnode{name: "f", args: []Node{
				numbernode{value: 1},
				stringnode{value: `'string'`},
				identnode{name: `foo`},
			}}},
		}}},
		{in: []byte(`SELECT f(g())`), out: selectnode{columns: []columnnode{
			{node: callnode{name: "f", args: []Node{
				callnode{name: "g"},
			}}},
		}}},
		{in: []byte(`SELECT * FROM foo`), out: selectnode{
			columns: []columnnode{
				{node: identnode{name: "*"}},
			},
			fromitems: []fromitemnode{
				{node: identnode{name: "foo"}},
			},
		}},
		{in: []byte(`SELECT * FROM "foo", bar`), out: selectnode{
			columns: []columnnode{
				{node: identnode{name: "*"}},
			},
			fromitems: []fromitemnode{
				{node: identnode{name: `"foo"`}},
				{node: identnode{name: "bar"}},
			},
		}},
		{in: []byte(`SELECT * FROM generate_series(1, 2)`), out: selectnode{
			columns: []columnnode{{node: identnode{name: "*"}}},
			fromitems: []fromitemnode{
				{node: callnode{name: "generate_series", args: []Node{
					numbernode{value: 1}, numbernode{value: 2},
				}}},
			},
		}},
		{in: []byte(`SELECT * FROM generate_series(1, 2) as "foo"`), out: selectnode{
			columns: []columnnode{{node: identnode{name: "*"}}},
			fromitems: []fromitemnode{
				{node: callnode{name: "generate_series", args: []Node{
					numbernode{value: 1}, numbernode{value: 2},
				}}, as: `"foo"`},
			},
		}},
	} {
		t.Run(string(tt.in), func(t *testing.T) {
			node, err := Parse(tt.in)
			if err != nil {
				t.Fatal(err)
			}
			assertf(t, reflect.DeepEqual(tt.out, node), "expected %+v, got %+v", tt.out, node)
		})
	}
}
