package connector

import (
	"errors"
	"sync"
	"time"

	. "github.com/beatuslapis/gorelib.v0/connector/cluster"

	"github.com/mediocregopher/radix.v2/redis"
)

// An interface to read informations about nodes with consisting a cluster
type NodeReader interface {
	GetShards() []Shard
}

// A function used when status changes by the healthchecker
type StatusFunc func(s *Shard, status int)

// An interface for a healthchecker to manage cluster status'
type HealthChecker interface {
	Start(shards []Shard, setStatus StatusFunc)
	Stop()
}

// An option stucture to create a cluster connector
type ClusterOptions struct {
	Reader NodeReader
	Builder RingBuilder
	Checker HealthChecker
	Poolsize int
	Failover bool
}

// A connector with clusterized redis instances.
type Cluster struct {
	mx sync.RWMutex
	shards []Shard
	ring *HashRing
	checker HealthChecker
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
	cluster := &Cluster{}
	if err := cluster.Reload(options); err != nil {
		return nil, err
	}
	
	return cluster, nil
}

// Reload and refresh the cluster connector
func (c *Cluster) Reload(options *ClusterOptions) error {
	if options == nil || options.Reader == nil || options.Builder == nil {
		return errors.New("invalid options")
	}
	shards := options.Reader.GetShards()
	if shards == nil {
		return ErrReadShard
	}
	ring := options.Builder.Build(shards)
	if ring == nil {
		return ErrBuildRing
	}
	
	if c.checker != nil {
		c.checker.Stop()
	}

	c.mx.Lock()
	c.shards = shards
	c.ring = ring
	c.checker = options.Checker
	c.failover = options.Failover
	c.mx.Unlock()
	

	if c.checker != nil {
		go c.checker.Start(shards, func(s *Shard, st int) {
			c.mx.Lock()
			defer c.mx.Unlock()

			s.SetStatus(st)
		})
	}

	return nil
}

// Get a shard for a given key.
// If the cluster is enabled the shard failover option, it tries failover to an available shard.
// It is protected by the r/w mutex for concurrent executions.
func (c *Cluster) getShard(key []byte) (*Shard, error) {
	c.mx.RLock()
	defer c.mx.RUnlock()

	shard, next := c.ring.Get(key)
	for shard != nil {
		switch shard.GetStatus() {
		case StatusUnknown: return nil, ErrNotReady
		case StatusUp: return shard, nil
		default:
			if c.failover {
				shard = next()
			} else {
				shard = nil
			}
		}
	}
	
	return nil, ErrNotAvail
}

// Locate and connect to an appropriate redis instace with a key.
// Shard-to-shard failover could happen when enabled and needed.
// If a located shard is not ready yet, i.e. the healthchecker does not decide its status,
// wait 0.1 second for settling down.
func (c *Cluster) Connect(key []byte) (*redis.Client, func(), int64, error) {
	shard, err := c.getShard(key)
	if err == ErrNotReady {
		for i := 0; err == ErrNotReady && i < 10; i++ {
			time.Sleep(100 * time.Millisecond)
			shard, err = c.getShard(key)
		}
	}
	if err != nil {
		return nil, nil, 0, err
	}
	
	client, serial, err := shard.GetClient()
	if err != nil {
		return nil, nil, 0, err
	}
	
	disconnect := func() {
		shard.ReleaseClient(client)
	}
	return client, disconnect, serial, nil
}