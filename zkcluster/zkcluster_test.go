package zkcluster

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	. "github.com/beatuslapis/gorelib.v0/checker"
	. "github.com/beatuslapis/gorelib.v0/connector/cluster"
)

func TestZKCluster(t *testing.T) {

	zkmanager, err := NewZKManager([]string{"localhost:2181"}, 10 * time.Second)
	if err != nil {
		t.Fatal("fail to create zkmanager:", err)
	}

	zkclusterinfo := &ZKClusterInfo{
		Name: "test",
		Options: ZKClusterOptions{
			RingType: "Consistent",
			RingParams: "4",
			FailoverEnabled: true,
		},
		Shards: []Shard{
			{
				Name: "node1",
				Addr: ":6378",
			},
			{
				Name: "node2",
				Addr: ":6379",
			},
		},
	}

	if err := zkmanager.CreateCluster(zkclusterinfo); err != nil {
		t.Fatal("failed to create the test cluster:", err)
	}

	status := make(map[string]ShardStatus, len(zkclusterinfo.Shards))
	for _, shard := range zkclusterinfo.Shards {
		status[shard.Addr] = ShardStatus{
			Addr: shard.Addr,
			Alive: false,
		}
	}
	fmt.Println("Status of shards:", status)

	statusbytes, _ := json.Marshal(status)
	zkmanager.zc.conn.Create(ZK_ROOT + "/test/status", statusbytes, DEF_FLAGS, DEF_ACL)

	zkcluster, err := NewZKCluster([]string{"localhost:2181"}, "test", 10 * time.Second)
	if err != nil {
		t.Fatal("fail to create zkcluster:", err)
	}

	if client, disconnect, _, err := zkcluster.Connect([]byte("test")); err != nil {
		fmt.Println("connect error:", err)
	} else {
		resp := client.Cmd("PING")
		fmt.Println(resp)
		disconnect()
		t.Fail()
	}

	zkcheckeroptions := ZKCheckerOptions{
		ZKServers: []string{"localhost:2181"},
		ZKTimeout: 10 * time.Second,
		Clustername: "test",
		Nworker: 2,
		Interval: 1 * time.Second,
		Threshold: 2 * time.Second,
	}

	zkchecker, _ := NewZKChecker(&zkcheckeroptions)
	zkchecker.Start()

	zkchecker2, _ := NewZKChecker(&zkcheckeroptions)
	zkchecker2.Start()

	zkchecker3, _ := NewZKChecker(&zkcheckeroptions)
	zkchecker3.Start()

	time.Sleep(3 * time.Second)

	if client, disconnect, _, err := zkcluster.Connect([]byte("test")); err != nil {
		fmt.Println("connect error:", err)
		t.Fail()
	} else {
		resp := client.Cmd("PING")
		fmt.Println(resp)
		disconnect()
	}

	zkcluster.Shutdown()
	time.Sleep(1 * time.Second)

	zkchecker.Shutdown()
	time.Sleep(1 * time.Second)

	zkchecker3.Shutdown()
	time.Sleep(1 * time.Second)

	zkchecker2.Shutdown()
	time.Sleep(1 * time.Second)

	if err := zkmanager.DeleteCluster("test"); err != nil {
		t.Fatal("failed to delete the test cluster:", err)
	}

	zkmanager.Shutdown()
}
