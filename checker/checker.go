package checker

import (
	. "github.com/beatuslapis/gorelib.v0/connector/cluster"
)

type ShardStatus struct {
	Addr string
	Alive bool
	Since int64
}

// An interface for a health checker to manage cluster status
type HealthChecker interface {
	Start([]Shard) <-chan ShardStatus
	Stop()
}

