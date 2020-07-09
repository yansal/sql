package preload

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"testing"

	"github.com/yansal/sql/build"
)

func assertf(t *testing.T, ok bool, msg string, args ...interface{}) {
	t.Helper()
	if !ok {
		t.Errorf(msg, args...)
	}
}
func assertValuesEqual(t *testing.T, values, expect []driver.Value) {
	t.Helper()
	assertf(t, len(values) == len(expect), "expected %d values, got %d", len(expect), len(values))
	for i := range expect {
		assertContains(t, values, expect[i])
	}
}

func assertContains(t *testing.T, slice []driver.Value, value driver.Value) {
	t.Helper()
	for i := range slice {
		if slice[i] == value {
			return
		}
	}
	assertf(t, false, "expected values to contain %v, got %v", value, slice)
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
	for i, v := range r.values[0] {
		values[i] = v
	}
	r.values = r.values[1:]
	return nil
}

type Order struct {
	ID                int64         `scan:"id"`
	CustomerID        int64         `scan:"customer_id"`
	ShippingAddressID sql.NullInt64 `scan:"shipping_address_id"`

	Customer        *Customer        `preload:"customers.id = customer_id"`
	ShippingAddress *ShippingAddress `preload:"shipping_addresses.id = shipping_address_id"`
}

type Customer struct {
	ID int64 `scan:"id"`

	Orders []Order `preload:"orders.customer_id = id"`
}

type ShippingAddress struct {
	ID int64 `scan:"id"`
}

func TestStructPreloadPointer(t *testing.T) {
	ctx := context.Background()
	var customerID int64 = 1
	queryfunc := func(values []driver.Value) (driver.Rows, error) {
		expect := []driver.Value{customerID}
		assertValuesEqual(t, values, expect)
		return &mockRows{
			columns: []string{"id"},
			values:  [][]driver.Value{{1}},
		}, nil
	}
	preparefunc := func(query string) (driver.Stmt, error) {
		expect := `SELECT "id" FROM "customers" WHERE "id" IN ($1)`
		assertf(t, query == expect, "expected %q, got %q", expect, query)
		return &mockStmt{queryfunc: queryfunc}, nil
	}
	db := sql.OpenDB(&mockConnector{conn: &mockConn{preparefunc: preparefunc}})

	order := Order{CustomerID: customerID}
	if err := Struct(ctx, db, &order, []Field{
		{Name: "Customer"},
	}); err != nil {
		t.Fatal(err)
	}

	assertf(t, order.Customer != nil, "expected order.Customer to not be nil")
	assertf(t, order.Customer.ID == order.CustomerID,
		"expected order.Customer.ID to equal %d, got %d", order.CustomerID, order.Customer.ID)
}

func TestStructPreloadSlice(t *testing.T) {
	ctx := context.Background()
	var (
		customerID int64 = 1
		order1ID   int64 = 2
		order2ID   int64 = 3
	)
	queryfunc := func(values []driver.Value) (driver.Rows, error) {
		expect := []driver.Value{customerID}
		assertValuesEqual(t, values, expect)
		return &mockRows{
			columns: []string{"id", "customer_id"},
			values: [][]driver.Value{
				{order1ID, customerID},
				{order2ID, customerID},
			},
		}, nil
	}
	preparefunc := func(query string) (driver.Stmt, error) {
		expect := `SELECT "id", "customer_id", "shipping_address_id" FROM "orders" WHERE "customer_id" IN ($1)`
		assertf(t, query == expect, "expected %q, got %q", expect, query)
		return &mockStmt{queryfunc: queryfunc}, nil
	}
	db := sql.OpenDB(&mockConnector{conn: &mockConn{preparefunc: preparefunc}})

	customer := Customer{ID: customerID}
	if err := Struct(ctx, db, &customer, []Field{{Name: "Orders"}}); err != nil {
		t.Fatal(err)
	}

	assertf(t, len(customer.Orders) == 2,
		"expected customer.Orders to have 2 items, got %d", len(customer.Orders))
	assertf(t, customer.Orders[0].CustomerID == customer.ID,
		"expected customer.Orders[0].CustomerID to equal %d, got %d", customer.ID, customer.Orders[0].CustomerID)
	assertf(t, customer.Orders[0].ID == order1ID,
		"expected customer.Orders[0].ID to equal %d, got %d", order1ID, customer.Orders[0].ID)
	assertf(t, customer.Orders[1].CustomerID == customer.ID,
		"expected customer.Orders[1].CustomerID to equal %d, got %d", customer.ID, customer.Orders[1].CustomerID)
	assertf(t, customer.Orders[1].ID == order2ID,
		"expected customer.Orders[1].ID to equal %d, got %d", order2ID, customer.Orders[1].ID)
}

func TestStructSlicePreloadPointer(t *testing.T) {
	ctx := context.Background()
	var (
		customer1ID int64 = 1
		customer2ID int64 = 2
	)
	queryfunc := func(values []driver.Value) (driver.Rows, error) {
		expect := []driver.Value{customer1ID, customer2ID}
		assertValuesEqual(t, values, expect)
		return &mockRows{
			columns: []string{"id"},
			values: [][]driver.Value{
				{customer1ID},
				{customer2ID},
			},
		}, nil
	}
	preparefunc := func(query string) (driver.Stmt, error) {
		expect := `SELECT "id" FROM "customers" WHERE "id" IN ($1, $2)`
		assertf(t, query == expect, "expected %q, got %q", expect, query)
		return &mockStmt{queryfunc: queryfunc}, nil
	}
	db := sql.OpenDB(&mockConnector{conn: &mockConn{preparefunc: preparefunc}})

	orders := []Order{
		{CustomerID: customer1ID},
		{CustomerID: customer2ID},
	}
	if err := StructSlice(ctx, db, orders, []Field{{Name: "Customer"}}); err != nil {
		t.Fatal(err)
	}
	for i := range orders {
		assertf(t, orders[i].Customer != nil, "expected orders[%d].Customer to not be nil", i)
		assertf(t, orders[i].Customer.ID == orders[i].CustomerID,
			"expected orders[%d].Customer.ID to equal %d, got %d", i, orders[i].CustomerID, orders[i].Customer.ID)
	}
}

func TestStructSlicePreloadSlice(t *testing.T) {
	ctx := context.Background()
	var (
		customer1ID int64 = 1
		customer2ID int64 = 2
		order1ID    int64 = 3
		order2ID    int64 = 4
		order3ID    int64 = 5
	)
	queryfunc := func(values []driver.Value) (driver.Rows, error) {
		expect := []driver.Value{customer1ID, customer2ID}
		assertValuesEqual(t, values, expect)
		return &mockRows{
			columns: []string{"id", "customer_id"},
			values: [][]driver.Value{
				{order1ID, customer1ID},
				{order2ID, customer2ID},
				{order3ID, customer2ID},
			},
		}, nil
	}
	preparefunc := func(query string) (driver.Stmt, error) {
		expect := `SELECT "id", "customer_id", "shipping_address_id" FROM "orders" WHERE "customer_id" IN ($1, $2)`
		assertf(t, query == expect, "expected %q, got %q", expect, query)
		return &mockStmt{queryfunc: queryfunc}, nil
	}
	db := sql.OpenDB(&mockConnector{conn: &mockConn{preparefunc: preparefunc}})

	customers := []Customer{
		{ID: customer1ID},
		{ID: customer2ID},
	}
	if err := StructSlice(ctx, db, customers, []Field{{Name: "Orders"}}); err != nil {
		t.Fatal(err)
	}

	assertf(t, len(customers[0].Orders) == 1,
		"expected customers[0].Orders to have 1 items, got %d", len(customers[0].Orders))
	assertf(t, customers[0].Orders[0].CustomerID == customers[0].ID,
		"expected customers[0].Orders[0].CustomerID to equal %d, got %d", customers[0].ID, customers[0].Orders[0].CustomerID)
	assertf(t, customers[0].Orders[0].ID == order1ID,
		"expected customers[0].Orders[0].ID to equal %d, got %d", order1ID, customers[0].Orders[0].ID)
	assertf(t, len(customers[1].Orders) == 2,
		"expected customers[1].Orders to have 2 items, got %d", len(customers[1].Orders))
	assertf(t, customers[1].Orders[0].CustomerID == customers[1].ID,
		"expected customers[1].Orders[0].CustomerID to equal %d, got %d", customers[1].ID, customers[1].Orders[0].CustomerID)
	assertf(t, customers[1].Orders[0].ID == order2ID,
		"expected customers[1].Orders[0].ID to equal %d, got %d", order2ID, customers[1].Orders[0].ID)
	assertf(t, customers[1].Orders[1].CustomerID == customers[1].ID,
		"expected customers[1].Orders[1].CustomerID to equal %d, got %d", customers[1].ID, customers[1].Orders[1].CustomerID)
	assertf(t, customers[1].Orders[1].ID == order3ID,
		"expected customers[1].Orders[1].ID to equal %d, got %d", order3ID, customers[1].Orders[1].ID)
}

func TestSqlNull(t *testing.T) {
	ctx := context.Background()
	var done bool
	preparefunc := func(query string) (driver.Stmt, error) {
		done = true
		return &mockStmt{}, nil
	}
	db := sql.OpenDB(&mockConnector{conn: &mockConn{preparefunc: preparefunc}})

	order := Order{}
	if err := Struct(ctx, db, &order, []Field{{Name: "ShippingAddress"}}); err != nil {
		t.Fatal(err)
	}

	assertf(t, !done, "expected no query")
	assertf(t, order.ShippingAddress == nil, "expected order.ShippingAddress to be nil")
}

func TestSqlNotNull(t *testing.T) {
	ctx := context.Background()
	var shippingAddressID int64 = 1
	queryfunc := func(values []driver.Value) (driver.Rows, error) {
		expect := []driver.Value{shippingAddressID}
		assertValuesEqual(t, values, expect)
		return &mockRows{
			columns: []string{"id"},
			values:  [][]driver.Value{{shippingAddressID}},
		}, nil
	}
	preparefunc := func(query string) (driver.Stmt, error) {
		expect := `SELECT "id" FROM "shipping_addresses" WHERE "id" IN ($1)`
		assertf(t, query == expect, "expected %q, got %q", expect, query)
		return &mockStmt{queryfunc: queryfunc}, nil
	}
	db := sql.OpenDB(&mockConnector{conn: &mockConn{preparefunc: preparefunc}})

	order := Order{ShippingAddressID: sql.NullInt64{Int64: shippingAddressID, Valid: true}}
	if err := Struct(ctx, db, &order, []Field{{Name: "ShippingAddress"}}); err != nil {
		t.Fatal(err)
	}

	assertf(t, order.ShippingAddress != nil, "expected order.ShippingAddress to be not nil")
	assertf(t, order.ShippingAddress.ID == order.ShippingAddressID.Int64,
		"expected order.ShippingAddress to equal %d, got %d", order.ShippingAddressID.Int64, order.ShippingAddress.ID)
}

func TestUniqueBindValues(t *testing.T) {
	ctx := context.Background()
	var (
		customerID int64 = 1
	)
	queryfunc := func(values []driver.Value) (driver.Rows, error) {
		expect := []driver.Value{customerID}
		assertValuesEqual(t, values, expect)
		return &mockRows{
			columns: []string{"id"},
			values: [][]driver.Value{
				{customerID},
			},
		}, nil
	}
	preparefunc := func(query string) (driver.Stmt, error) {
		expect := `SELECT "id" FROM "customers" WHERE "id" IN ($1)`
		assertf(t, query == expect, "expected %q, got %q", expect, query)
		return &mockStmt{queryfunc: queryfunc}, nil
	}
	db := sql.OpenDB(&mockConnector{conn: &mockConn{preparefunc: preparefunc}})

	orders := []Order{
		{CustomerID: customerID},
		{CustomerID: customerID},
	}
	if err := StructSlice(ctx, db, orders, []Field{{Name: "Customer"}}); err != nil {
		t.Fatal(err)
	}
	for i := range orders {
		assertf(t, orders[i].Customer != nil, "expected orders[%d].Customer to not be nil", i)
		assertf(t, orders[i].Customer.ID == orders[i].CustomerID,
			"expected orders[%d].Customer.ID to equal %d, got %d", i, orders[i].CustomerID, orders[i].Customer.ID)
	}
}

func TestWhereOrderBy(t *testing.T) {
	ctx := context.Background()
	var customerID int64 = 1
	queryfunc := func(values []driver.Value) (driver.Rows, error) {
		expect := []driver.Value{customerID}
		assertValuesEqual(t, values, expect)
		return &mockRows{
			columns: []string{"id"},
			values:  [][]driver.Value{{1}},
		}, nil
	}
	preparefunc := func(query string) (driver.Stmt, error) {
		expect := `SELECT "id" FROM "customers" WHERE "id" IN ($1) AND "foo" = 'bar' ORDER BY "id"`
		assertf(t, query == expect, "expected %q, got %q", expect, query)
		return &mockStmt{queryfunc: queryfunc}, nil
	}
	db := sql.OpenDB(&mockConnector{conn: &mockConn{preparefunc: preparefunc}})

	order := Order{CustomerID: customerID}
	if err := Struct(ctx, db, &order, []Field{{
		Name:    "Customer",
		Where:   build.Ident("foo").Equal(build.String("bar")),
		OrderBy: []build.Expression{build.Ident("id")},
	}}); err != nil {
		t.Fatal(err)
	}

	assertf(t, order.Customer != nil, "expected order.Customer to not be nil")
	assertf(t, order.Customer.ID == order.CustomerID,
		"expected order.Customer.ID to equal %d, got %d", order.CustomerID, order.Customer.ID)
}

func TestNested(t *testing.T) {
	ctx := context.Background()

	var (
		customerID        int64 = 1
		order1ID          int64 = 2
		order2ID          int64 = 3
		shippingAddressID int64 = 4
	)
	queryvalues := [][]driver.Value{
		{customerID},
		{shippingAddressID},
	}
	queryrows := []driver.Rows{
		&mockRows{
			columns: []string{"id", "customer_id", "shipping_address_id"},
			values: [][]driver.Value{
				{order1ID, customerID, shippingAddressID},
				{order2ID, customerID, nil},
			},
		},
		&mockRows{
			columns: []string{"id"},
			values:  [][]driver.Value{{shippingAddressID}},
		},
	}
	var q int
	queryfunc := func(values []driver.Value) (driver.Rows, error) {
		expect := queryvalues[q]
		assertValuesEqual(t, values, expect)
		rows := queryrows[q]
		q++
		return rows, nil
	}

	queries := []string{
		`SELECT "id", "customer_id", "shipping_address_id" FROM "orders" WHERE "customer_id" IN ($1)`,
		`SELECT "id" FROM "shipping_addresses" WHERE "id" IN ($1)`,
	}
	var p int
	preparefunc := func(query string) (driver.Stmt, error) {
		expect := queries[p]
		assertf(t, query == expect, "expected %q, got %q", expect, query)
		p++
		return &mockStmt{queryfunc: queryfunc}, nil
	}
	db := sql.OpenDB(&mockConnector{conn: &mockConn{preparefunc: preparefunc}})
	customer := Customer{ID: customerID}
	if err := Struct(ctx, db, &customer, []Field{
		{Name: "Orders"},
		{Name: "Orders.ShippingAddress"},
	}); err != nil {
		t.Fatal(err)
	}
	assertf(t, len(customer.Orders) == 2,
		"expected customer.Orders to have 2 item, got %d", len(customer.Orders))

	assertf(t, customer.Orders[0].ID == order1ID,
		"expected customer.Orders[0].ID to equal %d, got %d", order1ID, customer.Orders[0].ID)
	assertf(t, customer.Orders[0].ShippingAddressID == sql.NullInt64{Int64: shippingAddressID, Valid: true},
		"expected customer.Orders[0].ShippingAddressID to equal %+v, got %+v",
		sql.NullInt64{Int64: shippingAddressID, Valid: true},
		customer.Orders[0].ShippingAddressID)
	assertf(t, customer.Orders[0].ShippingAddress != nil,
		"expected customer.Orders[0].ShippingAddress to not be nil")
	assertf(t, customer.Orders[0].ShippingAddress.ID == shippingAddressID,
		"expected customer.Orders[0].ShippingAddress.ID to equal %d, got %d",
		shippingAddressID,
		customer.Orders[0].ShippingAddress.ID)

	assertf(t, customer.Orders[1].ID == order2ID,
		"expected customer.Orders[1].ID to equal %d, got %d", order2ID, customer.Orders[1].ID)
	assertf(t, customer.Orders[1].ShippingAddressID == sql.NullInt64{},
		"expected customer.Orders[1].ShippingAddressID to not be valid")
	assertf(t, customer.Orders[1].ShippingAddress == nil,
		"expected customer.Orders[1].ShippingAddress to be nil")
}
