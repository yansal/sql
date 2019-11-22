package scan

import (
	"context"
	"database/sql"
)

// Queryer is the interface required by functions in the scan package.
type Queryer interface {
	ExecContext(context.Context, string, ...interface{}) (sql.Result, error)
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
}
