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
	case int, int64, string:
		b.params = append(b.params, value)
		fmt.Fprintf(&b.buf, "$%d", len(b.params))
	case []string:
		b.write("(")
		for i := range v {
			if i > 0 {
				b.write(", ")
			}
			b.bind(v[i])
		}
		b.write(")")
	default:
		panic(fmt.Sprintf("don't know how to bind value %#v (%T)", v, v))
	}
}

func (b *builder) write(s string) {
	b.buf.WriteString(s)
}
