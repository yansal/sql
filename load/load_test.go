package load

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"testing"
)

func assertf(t *testing.T, ok bool, msg string, args ...interface{}) {
	t.Helper()
	if !ok {
		t.Errorf(msg, args...)
	}
}
func assert(t *testing.T, ok bool) {
	t.Helper()
	assertf(t, ok, "expected condition to be true")
}

func assertequal[T comparable](t *testing.T, v1, v2 T) {
	t.Helper()
	assertf(t, v1 == v2, "expected %v to equal %v", v1, v2)
}

func assertlen[T any](t *testing.T, slice []T, l int) {
	t.Helper()
	assertf(t, len(slice) == l, "expected %v to have len %v", slice, l)
}

func assertvalueequal[T comparable](t *testing.T, v T, value driver.Value) {
	t.Helper()
	i, ok := value.(T)
	assertf(t, ok, "expected value %v of type %T to have type %T", value, value, v)
	assertequal(t, v, i)
}
func assertvaluescontains[T comparable](t *testing.T, v T, values []driver.Value) {
	t.Helper()
	for i := range values {
		vt, ok := values[i].(T)
		if !ok {
			continue
		}
		if v != vt {
			continue
		}
		return
	}
	assertf(t, false, "expected values %v to contain %v", values, v)
}

type mockConnector struct{ conn driver.Conn }

func (c *mockConnector) Connect(context.Context) (driver.Conn, error) { return c.conn, nil }
func (c *mockConnector) Driver() driver.Driver                        { return nil }

type mockConn struct {
	preparefunc func(string) (driver.Stmt, error)
}

func (c *mockConn) Begin() (driver.Tx, error) { return nil, nil }
func (c *mockConn) Close() error              { return nil }
func (c *mockConn) Prepare(query string) (driver.Stmt, error) {
	return c.preparefunc(query)
}

type mockStmt struct {
	queryfunc func([]driver.Value) (driver.Rows, error)
}

func (s *mockStmt) Close() error                               { return nil }
func (s *mockStmt) Exec([]driver.Value) (driver.Result, error) { return nil, nil }
func (s *mockStmt) NumInput() int                              { return -1 }
func (s *mockStmt) Query(values []driver.Value) (driver.Rows, error) {
	return s.queryfunc(values)
}

type mockRows struct {
	columns []string
	values  [][]driver.Value
}

func (r *mockRows) Close() error      { return nil }
func (r *mockRows) Columns() []string { return r.columns }
func (r *mockRows) Next(values []driver.Value) error {
	if len(r.values) == 0 {
		return io.EOF
	}
	if len(r.values[0]) != len(values) {
		panic(fmt.Sprintf("expected %d values, got %d", len(r.values[0]), len(values)))
	}
	copy(values, r.values[0])
	r.values = r.values[1:]
	return nil
}

type M struct {
	ID   int64
	Name string
	PMID sql.NullInt64
}

func (m *M) GetColumns() []string { return []string{"id", "name", "pm_id"} }
func (m *M) GetDests() []any      { return []any{&m.ID, &m.Name, &m.PMID} }
func (m *M) GetTable() string     { return "model" }

type PM struct {
	ID     int64
	MPtrID sql.NullInt64

	MPtr   *M
	MSlice []M
}

func (m *PM) GetPreloadBindValue(s string) any {
	switch s {
	case "MPtr":
		return m.MPtrID
	case "MSlice":
		return m.ID
	default:
		panic(fmt.Sprintf("unknown preload %q", s))
	}
}
func (m *PM) GetPreloadDestIdent(s string) string {
	switch s {
	case "MPtr":
		return "id"
	case "MSlice":
		return "pm_id"
	default:
		panic(fmt.Sprintf("unknown preload %q", s))
	}
}
func (m *PM) GetPreloadDestValue(s string, v any) any {
	switch s {
	case "MPtr":
		return v.(M).ID
	case "MSlice":
		return v.(M).PMID
	default:
		panic(fmt.Sprintf("unknown preload %q", s))
	}
}
func (m *PM) SetPreloadDest(s string, v any) {
	switch s {
	case "MPtr":
		m.MPtr = &v.([]M)[0]
	case "MSlice":
		m.MSlice = v.([]M)
	default:
		panic(fmt.Sprintf("unknown preload %q", s))
	}
}

type NM struct {
	PMPtr   *PM
	PMSlice []PM
}

func (m *NM) GetField(s string) any {
	switch s {
	case "PMPtr":
		return []PM{*m.PMPtr}
	case "PMSlice":
		return m.PMSlice
	default:
		panic(fmt.Sprintf("unknown field %q", s))
	}
}
func (m *NM) SetField(s string, v any) {
	switch s {
	case "PMPtr":
		m.PMPtr = &v.([]PM)[0]
	case "PMSlice":
		m.PMSlice = v.([]PM)
	default:
		panic(fmt.Sprintf("unknown field %q", s))
	}
}
