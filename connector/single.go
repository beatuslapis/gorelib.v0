package connector

import (
	"github.com/mediocregopher/radix.v2/pool"
	"github.com/mediocregopher/radix.v2/redis"
)

// A connector for a single redis instance
type Single struct {
	pool *pool.Pool
}

// Generate a connector for the given single redis instance
func NewSingle(addr string, poolsize int) (*Single, error) {
	c := &Single{}
	if pool, err := pool.New("tcp", addr, poolsize); err != nil {
		return nil, err
	} else {
		c.pool = pool
		return c, nil
	}
}

// Connect to a pooled single redis instance
func (c *Single) Connect(key []byte) (*redis.Client, func (), int64, error) {
	if client, err := c.pool.Get(); err != nil {
		return nil, nil, 0, err
	} else {
		return client, func() { c.pool.Put(client) }, 0, err
	}
}

// Dispose the connector
func (c *Single) Shutdown() {
	c.pool.Empty()
}
