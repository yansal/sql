package build

// Update returns a new UPDATE statement.
func Update(table string) *UpdateStmt {
	return &UpdateStmt{table: Ident(table)}
}

// Set adds a SET clause.
func (stmt *UpdateStmt) Set(assignments ...Assignment) *UpdateStmt {
	stmt.assignments = assignments
	return stmt
}

// From adds a FROM clause.
func (stmt *UpdateStmt) From(items ...Expression) *UpdateStmt {
	stmt.from = items
	return stmt
}

// Where adds a WHERE clause.
func (stmt *UpdateStmt) Where(condition Expression) *UpdateStmt {
	stmt.where = &where{Expression: condition}
	return stmt
}

// Returning adds a RETURNING clause.
func (stmt *UpdateStmt) Returning(exprs ...Expression) *UpdateStmt {
	stmt.returning = exprs
	return stmt
}

// Build builds stmt and its parameters.
func (stmt *UpdateStmt) Build() (string, []interface{}) {
	b := new(builder)
	stmt.build(b)
	return b.buf.String(), b.params
}

func (stmt *UpdateStmt) build(b *builder) {
	if stmt.ctes != nil {
		stmt.ctes.build(b)
	}

	b.write("UPDATE ")
	stmt.table.build(b)
	b.write(" SET ")
	for i := range stmt.assignments {
		if i > 0 {
			b.write(", ")
		}
		stmt.assignments[i].columnname.build(b)
		b.write(" = ")
		stmt.assignments[i].expr.build(b)
	}

	if stmt.from != nil {
		b.write(" ")
		stmt.from.build(b)
	}

	if stmt.where != nil {
		b.write(" ")
		stmt.where.build(b)
	}

	if stmt.returning != nil {
		b.write(" RETURNING ")
		stmt.returning.build(b)
	}
}

// A UpdateStmt is an UPDATE statement.
type UpdateStmt struct {
	ctes        *CTEs
	table       Expression
	assignments []Assignment
	from        from
	where       *where
	returning   selectexprs
}
