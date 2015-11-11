/*
Copyright 2013 Google Inc.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package cluster

import (
	"hash/crc32"
	"sort"
	"strconv"
)

// Consistent HashRing implementation
type ConsistentRing struct {
	Hashfunc HashFunc
	Nreplica int
}

// Build a consistent HashRing with given shard information
func (r *ConsistentRing) BuildRing(shards []Shard) *HashRing {
	fn := r.Hashfunc
	if fn == nil {
		fn = crc32.ChecksumIEEE
	}

	ring := &HashRing{
		hashfn: fn,
		points: make([]int, r.Nreplica * len(shards)),
		shards: make(map[int]*Shard, r.Nreplica * len(shards)),
	}
	for i, _ := range shards {
		for j := 0; j < r.Nreplica; j++ {
			hash := int(fn([]byte(strconv.Itoa(j) + shards[i].Name)))
			ring.points[i * r.Nreplica + j] = hash
			ring.shards[hash] = &shards[i]
		}
	}
	sort.Ints(ring.points)
	
	return ring
}