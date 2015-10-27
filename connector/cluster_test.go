package connector

import (
	"fmt"
	"testing"
)

type readncheck struct {
	nodes map[string]string
}

func (r *readncheck) GetShards() []Shard {
	shards := make([]Shard, len(r.nodes))
	i := 0
	for name, addr := range r.nodes {
		shards[i] = Shard{
			Name: name,
			Addr: addr,
		}
		i++
	}
	return shards
}

func (r *readncheck) Start(shards []Shard, setStatus StatusFunc) {
	for i, _ := range shards {
		if shards[i].Addr == ":6378" {
			setStatus(&shards[i], StatusDown)
		} else {
			setStatus(&shards[i], StatusUp)
		}
	}
}

func (r *readncheck) Stop() {
	return
}

func TestFailover(t *testing.T) {
	testNodes := make(map[string]string)
	testNodes["serverA"] = ":6378"
	testNodes["serverB"] = ":6379"
	
	readerAndChecker := &readncheck{
		nodes: testNodes,
	}
	
	options := &ClusterOptions{
		Reader: readerAndChecker,
		Builder: &ConsistentRing{
			Nreplica: 3,
		},
		Checker: readerAndChecker,
		Failover: true,
	}
	
	cluster, err := NewCluster(options)
	if err != nil {
		t.Fatal("can't create a cluster:", err)
	}

	client, disconnect, serial, err := cluster.Connect([]byte("test"))
	if err != nil {
		t.Fatal("can't connect to a node:", err, cluster.shards[0])
	}
	resp := client.Cmd("PING")
	fmt.Println(serial, resp)
	disconnect()
}