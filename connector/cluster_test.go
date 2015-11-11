package connector

import (
	"fmt"
	"testing"

	. "github.com/beatuslapis/gorelib.v0/checker"
	. "github.com/beatuslapis/gorelib.v0/connector/cluster"
)

type readncheck struct {
	nodes map[string]string
	updates chan ShardStatus
}

func (r *readncheck) ReadNodes() []Shard {
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

func (r *readncheck) Start(shards []Shard) <-chan ShardStatus {
	r.updates = make(chan ShardStatus)

	go func() {
		for _, s := range shards {
			if s.Addr == ":6378" {
				r.updates <- ShardStatus{
					Addr: s.Addr,
					Alive: false,
				}
			} else {
				r.updates <- ShardStatus{
					Addr: s.Addr,
					Alive: true,
				}
			}

		}
	}()

	return r.updates
}

func (r *readncheck) Stop() {
	close(r.updates)
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