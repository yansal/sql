package build

// WindowFunction returns a new window function.
func WindowFunction(function string, args ...Expression) WindowFunctionExpr {
	return WindowFunctionExpr{function: function, args: args}
}

// A WindowFunctionExpr is a window function expression.
type WindowFunctionExpr struct {
	function string
	args     []Expression
	over     *WindowDefinition
}

// Over sets the window defintion of w.
func (w WindowFunctionExpr) Over(def WindowDefinition) Expression {
	w.over = &def
	return w
}

func (w WindowFunctionExpr) build(b *builder) {
	b.write(w.function)
	b.write("(")
	for i, arg := range w.args {
		if i > 0 {
			b.write(", ")
		}
		arg.build(b)
	}
	b.write(") OVER (")
	if w.over != nil {
		b.write(" ")
		w.over.build(b)
		b.write(" ")
	}
	b.write(")")
}

// PartitionBy returns a window definition.
func PartitionBy(expr Expression) WindowDefinition {
	return WindowDefinition{partitionby: expr}
}

// OrderBy returns a window definition.
func OrderBy(exprs ...Expression) WindowDefinition {
	return WindowDefinition{orderby: exprs}
}

// A WindowDefinition is a window definition.
type WindowDefinition struct {
	partitionby Expression
	orderby     orderby
}

// OrderBy adds an ORDER BY clause to d.
func (d WindowDefinition) OrderBy(exprs ...Expression) WindowDefinition {
	d.orderby = exprs
	return d
}

func (d WindowDefinition) build(b *builder) {
	if d.partitionby != nil {
		b.write("PARTITION BY ")
		d.partitionby.build(b)
	}

	if d.partitionby != nil && d.orderby != nil {
		b.write(" ")
	}

	if d.orderby != nil {
		d.orderby.build(b)
	}
}
