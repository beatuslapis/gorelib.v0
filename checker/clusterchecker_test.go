package checker

import (
	"runtime"
	"fmt"
	"time"
	"testing"
	
	. "github.com/beatuslapis/gorelib.v0/connector"
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
	}
	
	checker := ClusterChecker {
		Nworker: 2,
		Interval: 1 * time.Second,
		Threshold: 3 * time.Second,
	}

	fmt.Println("Status:", shards[0].GetStatus(), ", ", shards[1].GetStatus())
	checker.Start(shards, func(s *Shard,status int){s.SetStatus(status)})
	for i := 0; i < 5; i++ {
		fmt.Println("------:", shards[0].GetStatus(), ", ", shards[1].GetStatus())
		time.Sleep(1 * time.Second)
	}
	checker.Stop()
	
	time.Sleep(2 * time.Second)
	fmt.Println(runtime.NumGoroutine())
}