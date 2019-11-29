package build

import (
	"bytes"
	"fmt"
)

type builder struct {
	buf    bytes.Buffer
	params []interface{}
}

func (b *builder) bind(value interface{}) {
	switch v := value.(type) {
	case []string:
		b.write("(")
		for i := range v {
			if i > 0 {
				b.write(", ")
			}
			b.bind(v[i])
		}
		b.write(")")
	case string:
		b.params = append(b.params, value)
		fmt.Fprintf(&b.buf, "$%d", len(b.params))
	default:
		panic(fmt.Sprintf("don't know how to bind value %#v (%T)", v, v))
	}
}

func (b *builder) write(s string) {
	b.buf.WriteString(s)
}

func (b *builder) build(i interface{}) {
	switch v := i.(type) {
	case string:
		b.write(v) // TODO: quote? how to make a distinction between column name and string constant?
	case *SelectStmt:
		b.write("(")
		v.build(b)
		b.write(")")
	case expression:
		v.build(b)
	default:
		panic(fmt.Sprintf("don't know how to build value %#v (%T)", v, v))
	}
}

type expression interface {
	build(*builder)
}
