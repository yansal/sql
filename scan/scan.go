package scan

import (
	"context"
	"database/sql"
)

// Queryer is the interface required by functions in the scan package.
//
// It is a copy of sql/driver.QueryerContext.
type Queryer interface {
	QueryContext(context.Context, string, ...interface{}) (*sql.Rows, error)
}
