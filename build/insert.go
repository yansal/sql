package build

import "fmt"

// InsertInto returns a new INSERT statement.
func InsertInto(table string, columns ...string) *InsertStmt {
	stmt := &InsertStmt{table: Ident(table)}
	if len(columns) > 0 {
		stmt.columns = make([]Expression, len(columns))
		for i := range columns {
			stmt.columns[i] = Ident(columns[i])
		}
	}
	return stmt
}

// DefaultValues adds the DEFAULT VALUES keyword.
func (stmt *InsertStmt) DefaultValues() *InsertStmt {
	stmt.values = defaultvalues{}
	return stmt
}

// Values adds VALUES.
func (stmt *InsertStmt) Values(values ...Expression) *InsertStmt {
	stmt.values = valuesexpr{values: values}
	return stmt
}

// Query adds a query.
func (stmt *InsertStmt) Query(query Expression) *InsertStmt {
	stmt.values = queryexpr{query: query}
	return stmt
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
func (stmt *InsertStmt) OnConflict(action ConflictAction) *InsertStmt {
	stmt.onconflict = &onconflictexpr{action: action}
	return stmt
}

// OnConflictTarget adds a ON CONFLICT clause with a conflict target.
func (stmt *InsertStmt) OnConflictTarget(target Expression, action ConflictAction) *InsertStmt {
	stmt.onconflict = &onconflictexpr{target: target, action: action}
	return stmt
}

// ConflictTarget returns a new ConflictTargetExpr.
func ConflictTarget(columns ...string) ConflictTargetExpr {
	exprs := make([]Expression, 0, len(columns))
	for i := range columns {
		exprs = append(exprs, Ident(columns[i]))
	}
	return ConflictTargetExpr{exprs: exprs}
}

// A ConflictTargetExpr is a conflict target expression.
type ConflictTargetExpr struct {
	exprs []Expression
}

func (e ConflictTargetExpr) build(b *builder) {
	for i := range e.exprs {
		if i != 0 {
			b.write(", ")
		}
		e.exprs[i].build(b)
	}
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
func (stmt *InsertStmt) Returning(exprs ...Expression) *InsertStmt {
	stmt.returning = exprs
	return stmt
}

// Build builds stmt and its parameters.
func (stmt *InsertStmt) Build() (string, []interface{}) {
	b := new(builder)
	stmt.build(b)
	return b.buf.String(), b.params
}

func (stmt *InsertStmt) build(b *builder) {
	b.write("INSERT INTO ")
	stmt.table.build(b)
	b.write(" ")

	if stmt.columns != nil {
		b.write("(")
		for i := range stmt.columns {
			if i > 0 {
				b.write(", ")
			}
			stmt.columns[i].build(b)
		}
		b.write(") ")
	}

	stmt.values.build(b)

	if stmt.onconflict != nil {
		b.write(" ")
		stmt.onconflict.build(b)
	}

	if stmt.returning != nil {
		b.write(" RETURNING ")
		stmt.returning.build(b)
	}
}

// A InsertStmt is a INSERT statement.
type InsertStmt struct {
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
