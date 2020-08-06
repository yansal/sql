package build

import "fmt"

// InsertInto builds a new INSERT command.
func InsertInto(table string) *InsertCmd {
	return &InsertCmd{table: Ident(table)}
}

// Values adds a VALUES clause.
func (cmd *InsertCmd) Values(values ...ColumnValue) *InsertCmd {
	cmd.values = values
	return cmd
}

// OnConflict adds a ON CONFLICT clause.
func (cmd *InsertCmd) OnConflict(action ConflictAction) *InsertCmd {
	cmd.onconflict = &onconflictexpr{action: action}
	return cmd
}

// OnConflictTarget adds a ON CONFLICT clause with a conflict target.
func (cmd *InsertCmd) OnConflictTarget(target string, action ConflictAction) *InsertCmd {
	cmd.onconflict = &onconflictexpr{target: Ident(target), action: action}
	return cmd
}

// Returning adds a RETURNING clause.
func (cmd *InsertCmd) Returning(exprs ...Expression) *InsertCmd {
	cmd.returning = exprs
	return cmd
}

// Build builds cmd and its parameters.
func (cmd *InsertCmd) Build() (string, []interface{}) {
	b := new(builder)
	cmd.build(b)
	return b.buf.String(), b.params
}

func (cmd *InsertCmd) build(b *builder) {
	b.write("INSERT INTO ")
	cmd.table.build(b)
	if len(cmd.values) == 0 {
		b.write(" DEFAULT VALUES")
	} else {
		b.write(" (")
		for i := range cmd.values {
			if i > 0 {
				b.write(", ")
			}
			cmd.values[i].column.build(b)
		}
		b.write(") VALUES (")
		for i := range cmd.values {
			if i > 0 {
				b.write(", ")
			}
			cmd.values[i].value.build(b)
		}
		b.write(")")
	}

	if cmd.onconflict != nil {
		b.write(" ")
		cmd.onconflict.build(b)
	}

	if cmd.returning != nil {
		b.write(" RETURNING ")
		cmd.returning.build(b)
	}
}

// A InsertCmd is a INSERT command.
type InsertCmd struct {
	table      Expression
	values     []ColumnValue
	onconflict *onconflictexpr
	returning  selectexprs
}

func Value(column string, value Expression) ColumnValue {
	return ColumnValue{
		column: Ident(column),
		value:  value,
	}
}

type ColumnValue struct {
	column Expression
	value  Expression
}

type onconflictexpr struct {
	target Expression
	action ConflictAction
}

func (e onconflictexpr) build(b *builder) {
	b.write("ON CONFLICT ")
	if e.target != nil {
		b.write("(")
		e.target.build(b)
		b.write(") ")
	}
	e.action.build(b)
}

// A ConflictAction is a conflict action.
type ConflictAction struct {
	do     conflictactiondo
	values []ColumnValue
}

func (a ConflictAction) build(b *builder) {
	switch do := a.do; do {
	case donothing:
		b.write("DO NOTHING")
	case doupdateset:
		b.write("DO UPDATE SET ")
		for i := range a.values {
			if i > 0 {
				b.write(", ")
			}
			a.values[i].column.build(b)
			b.write(" = ")
			a.values[i].value.build(b)
		}
	default:
		panic(fmt.Sprintf("unknown conflict action %d", do))
	}
}

// DoNothing is the DO NOTHING conflict_action.
var DoNothing = ConflictAction{do: donothing}

// DoUpdateSet is the DO UPDATE SET conflict_action.
func DoUpdateSet(values ...ColumnValue) ConflictAction {
	return ConflictAction{
		do:     doupdateset,
		values: values,
	}
}

type conflictactiondo int

const (
	donothing conflictactiondo = iota
	doupdateset
)
