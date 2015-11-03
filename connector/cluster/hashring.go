package cluster

import (
	"sort"
)

// An interface to build a HashRing with given shard informations
type RingBuilder interface {
	Build(shards []Shard) *HashRing
}

// Type of a hash function which would be used to create the ring and locate data
type HashFunc func([]byte) uint32

// HashRing object for a clusterized connector
type HashRing struct {
	hashfn HashFunc
	points []int
	shards map[int]*Shard
}

// Return a shard for a given key
// with the iterator closure function which could be used by failover logics
func (r *HashRing) Get(key []byte) (*Shard, func() *Shard) {
	if key == nil || r.points == nil || r.shards == nil {
		return nil, nil
	}
	hash := int(r.hashfn(key))
	idx := sort.Search(len(r.points), func(i int) bool { return r.points[i] >= hash })
	if idx >= len(r.points) {
		idx = 0
	}
	cur := idx
	wrapped := false
	
	next := func() *Shard {
		cur++
		if cur >= len(r.points) {
			wrapped = true
			cur = 0
		}
		if wrapped && cur >= idx {
			return nil
		}
		return r.shards[r.points[cur]]
	}

	return r.shards[r.points[idx]], next
}