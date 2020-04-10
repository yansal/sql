package build

// InsertInto builds a new INSERT command.
func InsertInto(table string) *InsertCmd {
	return &InsertCmd{table: Ident(table)}
}

// Values adds a VALUES clause.
func (cmd *InsertCmd) Values(values ...ColumnValue) *InsertCmd {
	cmd.values = values
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
	if cmd.returning != nil {
		b.write(" RETURNING ")
		cmd.returning.build(b)
	}
}

// A InsertCmd is a INSERT command.
type InsertCmd struct {
	table     Expression
	values    []ColumnValue
	returning selectexprs
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
