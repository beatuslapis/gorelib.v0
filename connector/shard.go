package connector

import (
	"errors"
	"time"
	
	"github.com/mediocregopher/radix.v2/pool"
	"github.com/mediocregopher/radix.v2/redis"
)

// Constants describing the status of a cluster shard
const (
	StatusUnknown = iota
	StatusDown
	StatusUp
)

// Shard struct representing a shard of clusterized redis instances
type Shard struct {
	Name string
	Addr string
	Poolsize int
	pool *pool.Pool
	status int
	since int64
}

// Return a status of a shard
func (s *Shard) GetStatus() int {
	return s.status
}

// Set a status of a shard
func (s *Shard) SetStatus(status int) {
	s.SetStatusAndSince(status, time.Now().UnixNano() / 1000)
}

// Set a status of a shard with adjusting its valid timestamp in millis
func (s *Shard) SetStatusAndSince(status int, since int64) {
	s.status = status
	s.since = since
}

// Get a client of a shard which is pooled by radix.pool
func (s *Shard) GetClient() (*redis.Client, int64, error) {
	if s.status != StatusUp {
		s.Close()
		return nil, 0, errors.New("The shard is not available")
	}

	if s.pool == nil {
		if pool, err := pool.New("tcp", s.Addr, s.Poolsize); err != nil {
			return nil, 0, err
		} else {
			s.pool = pool
		}
	}
	
	if client, err := s.pool.Get(); err != nil {
		return nil, 0, err
	} else {
		return client, s.since, nil
	}
}

// Release the client and put it back to the pool
func (s *Shard) ReleaseClient(client *redis.Client) {
	if s.pool != nil {
		s.pool.Put(client)
	}
	return
}

// Clean up the pool of a shard
func (s *Shard) Close() {
	if s.pool != nil {
		s.pool.Empty()
		s.pool = nil
	}
}