package load

import (
	"context"
	"database/sql"
)

type Model interface {
	GetColumns() []string
	GetTable() string
	GetDests() []any
}

type Querier interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}
