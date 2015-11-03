package zkcluster

import (
	"testing"
	"fmt"
	"time"
)

func TestGetShard(t *testing.T) {
	zkcluster, err := NewZKCluster([]string{"localhost:2181"}, "test", 10 * time.Second)
	if err != nil {
		t.Fatal("fail to create zkcluster:", err)
	}

	shards := zkcluster.GetShards()
	fmt.Println(shards)
}
