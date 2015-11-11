package connector

import (
	"github.com/mediocregopher/radix.v2/redis"
)

// Connector inteface to get a redis client
type Connector interface {
	// Connect takes a key of []byte form.
	// It resturns a client for the key with its disconnect function,
	// also its validity serial which could be used
	// for the cache invalidation, possibly consistent, checks.
	Connect([]byte) (*redis.Client, func(), int64, error)

	// Dispose the connector
	Shutdown()
}
