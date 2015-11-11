package cluster

import (
	"fmt"
	"strconv"
	"testing"
)

var dummyhash = func (key []byte) uint32 {
	if num, err := strconv.Atoi(string(key)); err == nil {
		return uint32(num)
	}
	return 0
}

var keys = []string{
	"1000",
	"1200",
	"1800",
	"2200",
	"2300",
	"3400",
}

type ReaderAndBuilder struct {
	ConsistentRing
	nodes map[string]string
}

func (rb *ReaderAndBuilder) GetShards() []Shard {
	shards := make([]Shard, len(rb.nodes))
	i := 0
	for name, addr := range rb.nodes {
		shards[i] = Shard{
			Name: name,
			Addr: addr,
		}
		i++
	}
	return shards
}

func TestWrap(t *testing.T) {
	var nodelist = make(map[string]string)
	nodelist["111"] = "server1"
	nodelist["222"] = "server2"
	nodelist["333"] = "server3"

	builder := &ReaderAndBuilder{
		ConsistentRing{
			Nreplica: 3,
			Hashfunc: dummyhash,
		},
		nodelist,
	}
	ring := builder.BuildRing(builder.GetShards())
	a, nextA := ring.Get([]byte("2300"))
	i := 0
	for ; a != nil; i++ {
		fmt.Println(i, ":", a.Addr)
		a = nextA()
	}
	if i != 9 {
		fmt.Println("incomplete iteration")
		t.Fail()
	}
}

func TestConsistency(t *testing.T) {
	var nodelist = make(map[string]string)
	nodelist["222"] = "server2"
	nodelist["333"] = "server3"

	builderB := &ReaderAndBuilder{
		ConsistentRing{
			Nreplica: 3,
			Hashfunc: dummyhash,
		},
		nodelist,
	}
	ringB := builderB.BuildRing(builderB.GetShards())

	nodelist["111"] = "server1"
	builderA := &ReaderAndBuilder{
		ConsistentRing{
			Nreplica: 3,
			Hashfunc: dummyhash,
		},
		nodelist,
	}
	ringA := builderA.BuildRing(builderA.GetShards())

	for _, key := range keys {
		a, nextA := ringA.Get([]byte(key))
		b, _ := ringB.Get([]byte(key))
		fmt.Println("Key:", key, " A:", a.Name, " B:", b.Name)
		for a.Name == "111" {
			a = nextA()
			fmt.Println("          nextA:", a.Name)
		}
		if a.Name != b.Name {
			fmt.Println("inconsistency found")
			t.Fail()
		}
	}
}