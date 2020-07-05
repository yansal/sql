package build

import (
	"bytes"
	"database/sql/driver"
	"fmt"
	"time"
)

type builder struct {
	buf    bytes.Buffer
	params []interface{}
}

func (b *builder) bind(value interface{}) {
	switch v := value.(type) {
	case bool, float64, int, int64, string, []byte, time.Time, driver.Valuer:
		b.params = append(b.params, value)
		fmt.Fprintf(&b.buf, "$%d", len(b.params))
	case []int64:
		b.write("(")
		for i := range v {
			if i > 0 {
				b.write(", ")
			}
			b.bind(v[i])
		}
		b.write(")")
	case []string:
		b.write("(")
		for i := range v {
			if i > 0 {
				b.write(", ")
			}
			b.bind(v[i])
		}
		b.write(")")
	case []interface{}:
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
