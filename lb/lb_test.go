package lb

import (
	"context"
	"database/sql/driver"
	"testing"
)

type mockConnector struct {
	called int
}

func (c *mockConnector) Connect(ctx context.Context) (driver.Conn, error) {
	c.called++
	return nil, nil
}
func (c *mockConnector) Driver() driver.Driver { return nil }

func Test(t *testing.T) {
	conn1 := &mockConnector{}
	conn2 := &mockConnector{}
	lb := New(conn1, conn2)

	ctx := context.Background()
	if _, err := lb.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if conn1.called != 1 {
		t.Errorf("expected conn1's Connect method to be called once, been called %d times", conn1.called)
	}
	if conn2.called != 0 {
		t.Errorf("expected conn2's Connect method to not be called, been called %d times", conn2.called)
	}
	if _, err := lb.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if conn1.called != 1 {
		t.Errorf("expected conn1's Connect method to be called once, been called %d times", conn1.called)
	}
	if conn2.called != 1 {
		t.Errorf("expected conn2's Connect method to be called once, been called %d times", conn2.called)
	}
}

func TestRace(t *testing.T) {
	conn1 := &mockConnector{}
	conn2 := &mockConnector{}
	lb := New(conn1, conn2)

	ctx := context.Background()
	done := make(chan struct{})
	for i := 0; i < 2; i++ {
		go func() {
			if _, err := lb.Connect(ctx); err != nil {
				t.Fatal(err)
			}
			done <- struct{}{}
		}()
	}
	<-done
	<-done

	if conn1.called != 1 {
		t.Errorf("expected conn1's Connect method to be called once, been called %d times", conn1.called)
	}
	if conn2.called != 1 {
		t.Errorf("expected conn2's Connect method to be called once, been called %d times", conn2.called)
	}
}
