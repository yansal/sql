package build

// WindowFunction returns a new window function.
func WindowFunction(function string, args ...Expression) WindowFunctionExpr {
	return windowFunctionExpr{function: function, args: args}
}

// A WindowFunctionExpr is a window function expression.
type WindowFunctionExpr interface {
	Expression
	Over(WindowExpr) Expression
}

type windowFunctionExpr struct {
	function   string
	args       []Expression
	windowExpr WindowExpr
}

func (w windowFunctionExpr) Over(windowExpr WindowExpr) Expression {
	w.windowExpr = windowExpr
	return w
}

func (w windowFunctionExpr) build(b *builder) {
	b.write(w.function)
	b.write("(")
	for i, arg := range w.args {
		if i > 0 {
			b.write(", ")
		}
		arg.build(b)
	}
	b.write(")")
	if w.windowExpr.partitionBy != nil {
		b.write(" OVER (PARTITION BY ")
		w.windowExpr.partitionBy.build(b)
		b.write(")")
	}
}

// PartitionBy returns a new WindowExpr.
func PartitionBy(expr Expression) WindowExpr {
	return WindowExpr{partitionBy: expr}
}

// A WindowExpr is a window expression.
type WindowExpr struct {
	partitionBy Expression
}
