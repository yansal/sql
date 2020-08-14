package build

// Update builds a new UPDATE command.
func Update(table string) *UpdateCmd {
	return &UpdateCmd{table: Ident(table)}
}

// Set adds a SET clause.
func (cmd *UpdateCmd) Set(assignments ...Assignment) *UpdateCmd {
	cmd.assignments = assignments
	return cmd
}

// Where adds a WHERE clause.
func (cmd *UpdateCmd) Where(condition Expression) *UpdateCmd {
	cmd.where = &where{Expression: condition}
	return cmd
}

// Returning adds a RETURNING clause.
func (cmd *UpdateCmd) Returning(exprs ...Expression) *UpdateCmd {
	cmd.returning = exprs
	return cmd
}

// Build builds cmd and its parameters.
func (cmd *UpdateCmd) Build() (string, []interface{}) {
	b := new(builder)
	cmd.build(b)
	return b.buf.String(), b.params
}

func (cmd *UpdateCmd) build(b *builder) {
	b.write("UPDATE ")
	cmd.table.build(b)
	b.write(" SET ")
	for i := range cmd.assignments {
		if i > 0 {
			b.write(", ")
		}
		cmd.assignments[i].columnname.build(b)
		b.write(" = ")
		cmd.assignments[i].expr.build(b)
	}

	if cmd.where != nil {
		b.write(" ")
		cmd.where.build(b)
	}

	if cmd.returning != nil {
		b.write(" RETURNING ")
		cmd.returning.build(b)
	}
}

// A UpdateCmd is an UPDATE command.
type UpdateCmd struct {
	table       Expression
	assignments []Assignment
	where       *where
	returning   selectexprs
}
