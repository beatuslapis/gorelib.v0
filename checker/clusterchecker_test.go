package checker

import (
	"sync"
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
	
	checker := ClusterChecker {
		Nworker: 2,
		Interval: 1 * time.Second,
		Threshold: 3 * time.Second,
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 60; i++ {
			fmt.Print("[", i, "] Status: ")
			for j, _ := range shards {
				fmt.Print(shards[j].GetStatus(), ", ")
			}
			fmt.Println("")
			time.Sleep(1 * time.Second)
		}
	}()
	
	checker.Start(shards, func(s *Shard,status int){s.SetStatus(status)})
	time.Sleep(60 * time.Second)

	checker.Stop()
	time.Sleep(2 * time.Second)
	
	wg.Wait()
}