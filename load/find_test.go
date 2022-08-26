package load

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"
)

func TestFind(t *testing.T) {
	ctx := context.Background()
	var (
		id1, id2     int64  = 1, 2
		name1, name2 string = "Yann", "Salaün"
	)
	queryfunc := func(values []driver.Value) (driver.Rows, error) {
		return &mockRows{
			columns: []string{"id", "name", "pm_id"},
			values: [][]driver.Value{
				{id1, name1, nil},
				{id2, name2, nil},
			},
		}, nil
	}
	preparefunc := func(query string) (driver.Stmt, error) {
		assertequal(t, `SELECT "model"."id", "model"."name", "model"."pm_id" FROM "model"`, query)
		return &mockStmt{queryfunc: queryfunc}, nil
	}
	db := sql.OpenDB(&mockConnector{conn: &mockConn{preparefunc: preparefunc}})

	ms, err := Find[M](ctx, db)
	if err != nil {
		t.Fatal(err)
	}

	assertlen(t, ms, 2)
	assertequal(t, id1, ms[0].ID)
	assertequal(t, name1, ms[0].Name)
	assertequal(t, id2, ms[1].ID)
	assertequal(t, name2, ms[1].Name)
}
