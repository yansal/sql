package load

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"testing"
)

func TestPreloadSliceNestedPtr(t *testing.T) {
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
	nesteds := []NM{
		{PMPtr: &PM{ID: pmid}},
	}
	if err := PreloadSliceNested[PM, M](ctx, db, nesteds, "PMPtr.MSlice"); err != nil {
		t.Fatal(err)
	}
	assertlen(t, nesteds[0].PMPtr.MSlice, 2)
	assertequal(t, m1, nesteds[0].PMPtr.MSlice[0])
	assertequal(t, m2, nesteds[0].PMPtr.MSlice[1])
}

func TestPreloadSliceNestedSlice(t *testing.T) {
	ctx := context.Background()
	var (
		pm1id int64 = 1
		pm2id int64 = 2
		m1          = M{ID: 3, Name: "Yann", PMID: sql.NullInt64{Int64: pm1id, Valid: true}}
		m2          = M{ID: 4, Name: "Salaün", PMID: sql.NullInt64{Int64: pm2id, Valid: true}}
	)
	queryfunc := func(values []driver.Value) (driver.Rows, error) {
		assertequal(t, 2, len(values))
		assertvaluescontains(t, m1.ID, values)
		assertvaluescontains(t, m2.ID, values)
		return &mockRows{
			columns: []string{"id", "name", "pm_id"},
			values: [][]driver.Value{
				{m1.ID, m1.Name, pm1id},
				{m2.ID, m2.Name, pm2id},
			},
		}, nil
	}
	preparefunc := func(query string) (driver.Stmt, error) {
		assertequal(t, `SELECT "model"."id", "model"."name", "model"."pm_id" FROM "model" WHERE "id" IN ($1, $2)`, query)
		return &mockStmt{queryfunc: queryfunc}, nil
	}
	db := sql.OpenDB(&mockConnector{conn: &mockConn{preparefunc: preparefunc}})
	nesteds := []NM{
		{
			PMSlice: []PM{
				{MPtrID: sql.NullInt64{Int64: m1.ID, Valid: true}},
				{MPtrID: sql.NullInt64{Int64: m2.ID, Valid: true}},
			},
		},
	}
	if err := PreloadSliceNested[PM, M](ctx, db, nesteds, "PMSlice.MPtr"); err != nil {
		t.Fatal(err)
	}
	assertlen(t, nesteds[0].PMSlice, 2)
	assertequal(t, m1, *nesteds[0].PMSlice[0].MPtr)
	assertequal(t, m2, *nesteds[0].PMSlice[1].MPtr)
}
