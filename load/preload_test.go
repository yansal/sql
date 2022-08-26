package load

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"
)

func TestPreloadPtr(t *testing.T) {
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
		assertequal(t, `SELECT "model"."id", "model"."name", "model"."pm_id" FROM "model" WHERE "id" IN ($1)`, query)
		return &mockStmt{queryfunc: queryfunc}, nil
	}
	db := sql.OpenDB(&mockConnector{conn: &mockConn{preparefunc: preparefunc}})

	pm := &PM{MPtrID: sql.NullInt64{Int64: id, Valid: true}}
	if err := PreloadPtr[M](ctx, db, pm, "MPtr"); err != nil {
		t.Fatal(err)
	}

	assertequal(t, id, pm.MPtr.ID)
	assertequal(t, name, pm.MPtr.Name)
}

func TestPreloadPtrSlice(t *testing.T) {
	ctx := context.Background()
	var (
		pmid int64 = 1
		m1         = M{ID: 2, Name: "Yann", PMID: sql.NullInt64{Int64: pmid, Valid: true}}
		m2         = M{ID: 3, Name: "Salaün", PMID: sql.NullInt64{Int64: pmid, Valid: true}}
	)
	queryfunc := func(values []driver.Value) (driver.Rows, error) {
		assertequal(t, 1, len(values))
		assertvalueequal(t, pmid, values[0])
		return &mockRows{
			columns: []string{"id", "name", "pm_id"},
			values: [][]driver.Value{
				{m1.ID, m1.Name, pmid},
				{m2.ID, m2.Name, pmid},
			},
		}, nil
	}
	preparefunc := func(query string) (driver.Stmt, error) {
		assertequal(t, `SELECT "model"."id", "model"."name", "model"."pm_id" FROM "model" WHERE "pm_id" IN ($1)`, query)
		return &mockStmt{queryfunc: queryfunc}, nil
	}
	db := sql.OpenDB(&mockConnector{conn: &mockConn{preparefunc: preparefunc}})

	pm := &PM{ID: pmid}
	if err := PreloadPtr[M](ctx, db, pm, "MSlice"); err != nil {
		t.Fatal(err)
	}

	assertlen(t, pm.MSlice, 2)
	assertequal(t, m1, pm.MSlice[0])
	assertequal(t, m2, pm.MSlice[1])
}

func TestPreloadSlice(t *testing.T) {
	ctx := context.Background()
	var (
		m1 = M{ID: 1, Name: "Yann"}
		m2 = M{ID: 2, Name: "Salaün"}
	)
	queryfunc := func(values []driver.Value) (driver.Rows, error) {
		assertequal(t, 2, len(values))
		assertvaluescontains(t, m1.ID, values)
		assertvaluescontains(t, m2.ID, values)
		return &mockRows{
			columns: []string{"id", "name", "pm_id"},
			values: [][]driver.Value{
				{m1.ID, m1.Name, nil},
				{m2.ID, m2.Name, nil},
			},
		}, nil
	}
	preparefunc := func(query string) (driver.Stmt, error) {
		assertequal(t, `SELECT "model"."id", "model"."name", "model"."pm_id" FROM "model" WHERE "id" IN ($1, $2)`, query)
		return &mockStmt{queryfunc: queryfunc}, nil
	}
	db := sql.OpenDB(&mockConnector{conn: &mockConn{preparefunc: preparefunc}})

	pms := []PM{
		{MPtrID: sql.NullInt64{Int64: m1.ID, Valid: true}},
		{MPtrID: sql.NullInt64{Int64: m2.ID, Valid: true}},
	}
	if err := PreloadSlice[M](ctx, db, pms, "MPtr"); err != nil {
		t.Fatal(err)
	}

	assertequal(t, m1.ID, pms[0].MPtr.ID)
	assertequal(t, m1.Name, pms[0].MPtr.Name)
	assertequal(t, m2.ID, pms[1].MPtr.ID)
	assertequal(t, m2.Name, pms[1].MPtr.Name)
}
