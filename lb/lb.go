package lb

import (
	"context"
	"database/sql/driver"
	"sync"
)

// New returns a new load-balancing connector.
func New(connectors ...driver.Connector) driver.Connector {
	return &connector{connectors: connectors}
}

type connector struct {
	sync.Mutex
	connectors []driver.Connector
	next       int
}

func (c *connector) Connect(ctx context.Context) (driver.Conn, error) {
	c.Lock()
	defer c.Unlock()
	c.next = c.next % len(c.connectors)
	conn, err := c.connectors[c.next].Connect(ctx)
	c.next++
	return conn, err
}

func (c *connector) Driver() driver.Driver {
	return c.connectors[0].Driver()
}
