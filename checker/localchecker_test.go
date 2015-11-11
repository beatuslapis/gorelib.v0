package checker

import (
	"fmt"
	"testing"
	"time"

	. "github.com/beatuslapis/gorelib.v0/connector/cluster"
)

func TestChecker(t *testing.T) {
	shards := []Shard{
		Shard{
			Name: "node1",
			Addr: ":6378",
		},
		Shard{
			Name: "node2",
			Addr: ":6379",
		},
		Shard{
			Name: "node3",
			Addr: ":6378",
		},
		Shard{
			Name: "node4",
			Addr: ":6379",
		},
		Shard{
			Name: "node5",
			Addr: ":6378",
		},
		Shard{
			Name: "node6",
			Addr: ":6379",
		},
	}
	
	checker := LocalChecker{
		Nworker: 2,
		Interval: 1 * time.Second,
		Threshold: 3 * time.Second,
	}

	updates := checker.Start(shards)
	go func() {
		for event := range updates {
			e := ShardStatus(event)
			fmt.Println(e.Addr, e)
		}
		fmt.Println("quitting reader...")
	}()
	time.Sleep(5 * time.Second)

	checker.Stop()
	time.Sleep(2 * time.Second)
}