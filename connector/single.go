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
func NewSingle(addr string, poolsize int) (c *Single, err error) {
	c = &Single{}
	c.pool, err = pool.New("tcp", addr, poolsize)
	if err != nil {
		c = nil
		return 
	}
	return
}

// Connect to a pooled single redis instance
func (c *Single) Connect (key []byte) (*redis.Client, func (), int64, error) {
	client, err := c.pool.Get()
	disconnect := func () {
		c.pool.Put(client)
	}
	return client, disconnect, 0, err
}

