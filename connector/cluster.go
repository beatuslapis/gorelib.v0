package connector

import (
	"errors"
	"sync"
	"time"

	. "github.com/beatuslapis/gorelib.v0/checker"
	. "github.com/beatuslapis/gorelib.v0/connector/cluster"

	"github.com/mediocregopher/radix.v2/pool"
	"github.com/mediocregopher/radix.v2/redis"
)

// An option structure to create a cluster connector
type ClusterOptions struct {
	Reader NodeReader
	Builder RingBuilder
	Checker HealthChecker
	Poolsize int
	Failover bool
}

// A connector with clustered redis instances.
type Cluster struct {
	mx sync.RWMutex

	shards []Shard
	ring *HashRing

	pool map[*Shard]*pool.Pool
	poolsize int

	checker HealthChecker
	status map[string]ShardStatus

	failover bool
}

// Error definitions
var (
	ErrNotReady = errors.New("The shard is not ready yet.")
	ErrNotAvail = errors.New("No shard is available for the key.")
	ErrReadShard = errors.New("Failed to read shard informations.")
	ErrBuildRing = errors.New("Failed to build a hash ring.")
)

// Generate a cluster connector with given options
func NewCluster(options *ClusterOptions) (*Cluster, error) {
	c := &Cluster{}

	if options == nil || options.Reader == nil || options.Builder == nil {
		return nil, errors.New("invalid options")
	}
	shards := options.Reader.ReadNodes()
	if shards == nil {
		return nil, ErrReadShard
	}
	ring := options.Builder.BuildRing(shards)
	if ring == nil {
		return nil, ErrBuildRing
	}

	if options.Poolsize > 0 {
		c.poolsize = options.Poolsize
	} else {
		c.poolsize = 4
	}
	c.shards = shards
	c.ring = ring
	c.pool = make(map[*Shard]*pool.Pool, len(c.shards))
	c.failover = options.Failover

	c.checker = options.Checker
	c.status = make(map[string]ShardStatus, len(c.shards))

	updates := c.checker.Start(c.shards)
	go c.statusUpdateReceiver(updates)

	return c, nil
}

func (c *Cluster) statusUpdateReceiver(updates <-chan ShardStatus) {
	for update := range updates {
		c.mx.Lock()
		c.status[update.Addr] = update
		c.mx.Unlock()
	}
}

var nilStatus = ShardStatus{}

// Get a shard for a given key.
// If the cluster is enabled the shard failover option, it tries failover to an available shard.
// It is protected by the r/w mutex for concurrent executions.
func (c *Cluster) getShard(key []byte) (*Shard, int64, error) {
	c.mx.RLock()
	defer c.mx.RUnlock()

	if c.ring == nil {
		return nil, 0, ErrNotAvail
	}

	shard, next := c.ring.Get(key)
	for shard != nil {
		if status := c.status[shard.Addr]; status == nilStatus {
			return nil, 0, ErrNotReady
		} else {
			if status.Alive {
				return shard, status.Since, nil
			} else if c.failover {
				shard = next()
			} else {
				shard = nil
			}
		}
	}
	
	return nil, 0, ErrNotAvail
}

// Locate and connect to an appropriate redis instace with a key.
// Shard-to-shard failover could happen when enabled and needed.
// If a located shard is not ready yet, i.e. the healthchecker does not decide its status,
// wait 0.1 second for settling down.
func (c *Cluster) Connect(key []byte) (*redis.Client, func(), int64, error) {
	shard, since, err := c.getShard(key)
	if err == ErrNotReady {
		for i := 0; err == ErrNotReady && i < 10; i++ {
			time.Sleep(100 * time.Millisecond)
			shard, since, err = c.getShard(key)
		}
	}
	if err != nil {
		return nil, nil, 0, err
	}

	cp := c.pool[shard]
	if cp == nil {
		if np, err := pool.New("tcp", shard.Addr, c.poolsize); err != nil {
			return nil, nil, 0, err
		} else {
			c.pool[shard] = np
			cp = np
		}
	}

	if client, err := cp.Get(); err == nil {
		return client, func(){ cp.Put(client) }, since, nil
	} else {
		return nil, nil, 0, err
	}
}

func (c *Cluster) Shutdown() {
	if c.checker != nil {
		c.checker.Stop()
	}

	c.mx.Lock()
	defer c.mx.Unlock()

	for i, _ := range c.shards {
		if p := c.pool[&c.shards[i]]; p != nil {
			c.pool[&c.shards[i]].Empty()
		}
	}
	c.shards = nil
	c.ring = nil
	c.checker = nil
}