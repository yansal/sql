package load

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"testing"

	"github.com/yansal/sql/build"
)

func TestGet(t *testing.T) {
	ctx := context.Background()
	var (
		id   int64  = 1
		name string = "Yann"
	)
	queryfunc := func(values []driver.Value) (driver.Rows, error) {
		assertequal(t, 1, len(values))
		assertvalueequal(t, id, values[0])
		return &mockRows{
			columns: []string{"id", "name", "pm_id"},
			values:  [][]driver.Value{{id, name, nil}},
		}, nil
	}
	preparefunc := func(query string) (driver.Stmt, error) {
		assertequal(t, `SELECT "id", "name", "pm_id" FROM "model" WHERE "id" = $1`, query)
		return &mockStmt{queryfunc: queryfunc}, nil
	}
	db := sql.OpenDB(&mockConnector{conn: &mockConn{preparefunc: preparefunc}})

	m, err := Get[M](ctx, db, build.Ident("id").Equal(build.Bind(id)))
	if err != nil {
		t.Fatal(err)
	}

	assertequal(t, id, m.ID)
	assertequal(t, name, m.Name)
}

func TestGetErrNotFound(t *testing.T) {
	ctx := context.Background()
	var id int64 = 1
	queryfunc := func(values []driver.Value) (driver.Rows, error) {
		assertequal(t, 1, len(values))
		assertvalueequal(t, id, values[0])
		return &mockRows{}, nil
	}
	preparefunc := func(query string) (driver.Stmt, error) {
		assertequal(t, `SELECT "id", "name", "pm_id" FROM "model" WHERE "id" = $1`, query)
		return &mockStmt{queryfunc: queryfunc}, nil
	}
	db := sql.OpenDB(&mockConnector{conn: &mockConn{preparefunc: preparefunc}})

	_, err := Get[M](ctx, db, build.Ident("id").Equal(build.Bind(id)))
	assert(t, errors.Is(err, sql.ErrNoRows))
}
