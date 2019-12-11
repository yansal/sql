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
		{in: []byte(`SELECT "foo"`), out: selectNode{columns: []selectExprNode{{ident: `"foo"`}}}},
		{in: []byte(`SELECT "foo", "bar"`), out: selectNode{columns: []selectExprNode{{ident: `"foo"`}, {ident: `"bar"`}}}},
	} {
		node, err := Parse(tt.in)
		if err != nil {
			t.Fatal(err)
		}
		assertf(t, reflect.DeepEqual(tt.out, node), "expected %+v, got %+v", tt.out, node)
	}
}
