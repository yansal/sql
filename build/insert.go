package build

import "fmt"

// InsertInto builds a new INSERT command.
func InsertInto(table string, columns ...string) *InsertCmd {
	cmd := &InsertCmd{table: Ident(table)}
	if len(columns) > 0 {
		cmd.columns = make([]Expression, len(columns))
		for i := range columns {
			cmd.columns[i] = Ident(columns[i])
		}
	}
	return cmd
}

// DefaultValues adds the DEFAULT VALUES keyword.
func (cmd *InsertCmd) DefaultValues() *InsertCmd {
	cmd.values = defaultvalues{}
	return cmd
}

// Values adds VALUES.
func (cmd *InsertCmd) Values(values ...Expression) *InsertCmd {
	cmd.values = valuesexpr{values: values}
	return cmd
}

// Query adds a query.
func (cmd *InsertCmd) Query(query Expression) *InsertCmd {
	cmd.values = queryexpr{query: query}
	return cmd
}

type defaultvalues struct{}

func (defaultvalues) build(b *builder) {
	b.write("DEFAULT VALUES")
}

type valuesexpr struct {
	values []Expression
}

func (e valuesexpr) build(b *builder) {
	b.write("VALUES (")
	for i := range e.values {
		if i > 0 {
			b.write(", ")
		}
		e.values[i].build(b)
	}
	b.write(")")
}

type queryexpr struct {
	query Expression
}

func (e queryexpr) build(b *builder) {
	e.query.build(b)
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
	values []Assignment
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
			a.values[i].columnname.build(b)
			b.write(" = ")
			a.values[i].expr.build(b)
		}
	default:
		panic(fmt.Sprintf("unknown conflict action %d", do))
	}
}

// DoNothing is the DO NOTHING conflict_action.
var DoNothing = ConflictAction{do: donothing}

// DoUpdateSet is the DO UPDATE SET conflict_action.
func DoUpdateSet(values ...Assignment) ConflictAction {
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
	b.write(" ")

	if cmd.columns != nil {
		b.write("(")
		for i := range cmd.columns {
			if i > 0 {
				b.write(", ")
			}
			cmd.columns[i].build(b)
		}
		b.write(") ")
	}

	cmd.values.build(b)

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
	columns    []Expression
	values     Expression
	onconflict *onconflictexpr
	returning  selectexprs
}

// Assign returns a new assignment.
func Assign(columnname string, expr Expression) Assignment {
	return Assignment{
		columnname: Ident(columnname),
		expr:       expr,
	}
}

// An Assignment is an assignment.
type Assignment struct {
	columnname Expression
	expr       Expression
}
