package zkcluster

import (

	. "github.com/beatuslapis/gorelib.v0/connector/cluster"

	"github.com/samuel/go-zookeeper/zk"
	"time"
	"encoding/json"
)

type ZKCluster struct {
	servers []string
	name string
	conn *zk.Conn
	events <-chan zk.Event
}

func NewZKCluster(servers []string, clustername string, timeout time.Duration) (*ZKCluster, error) {
	c := &ZKCluster{
		servers: servers,
		name: clustername,
	}

	if conn, events, err := zk.Connect(servers, timeout); err != nil {
		return nil, err
	} else {
		c.conn = conn
		c.events = events
	}

	return c, nil
}

func (c *ZKCluster) GetShards() []Shard {
	shardbytes, _, err := c.conn.Get("/goreclusters/" + c.name + "/shards")
	if err != nil {
		return nil
	}

	var shards []Shard
	if err := json.Unmarshal(shardbytes, shards); err != nil {
		return nil
	}

	return shards
}
