// zkcluster - A connector for a zookeeper assisted redis cluster.
// It reads cluster configurations and shard status from zookeeper servers.
// It has a checker implementation which could be run separately.
// Multiple checkers are also possible
// and they would automatically elect a leader and make quorum-like decisions.
package zkcluster

import (
	"encoding/json"
	"strconv"
	"strings"
	"time"

	. "github.com/beatuslapis/gorelib.v0/checker"
	. "github.com/beatuslapis/gorelib.v0/connector"
	. "github.com/beatuslapis/gorelib.v0/connector/cluster"

	"github.com/mediocregopher/radix.v2/redis"
	"github.com/samuel/go-zookeeper/zk"
)

// Connector implementation for the zookeeper assisted redis cluster.
// It reads cluster configurations and shard status from zookeeper servers.
// It also has roles of NodeReader, RingBuilder and HealthChecker which Cluster interface requires.
type ZKCluster struct {
	connector *Cluster

	zc *ZKConnector
	info *ZKClusterInfo

	version int32
	checkerdone chan bool
	checkerevent <-chan zk.Event

	status map[string]ShardStatus
	updates chan ShardStatus
}

// Return a new ZKCluster instance.
func NewZKCluster(servers []string, clustername string, timeout time.Duration) (*ZKCluster, error) {
	cluster := &ZKCluster{
		version: -1,
	}

	if zc, err := NewZKConnector(servers, timeout); err != nil {
		return nil, err
	} else {
		cluster.zc = zc
	}

	if ci, err := cluster.zc.GetCluster(clustername); err != nil {
		return nil, err
	} else {
		cluster.info = ci
	}

	if connector, err := NewCluster(&ClusterOptions{
		Reader: cluster,
		Builder: cluster,
		Checker: cluster,
		Failover: cluster.info.Options.FailoverEnabled,
	}); err != nil {
		return nil, err
	} else {
		cluster.connector = connector
	}

	return cluster, nil
}

// Locate and connect to an appropriate redis instance with a key.
func (c *ZKCluster) Connect(key []byte) (*redis.Client, func(), int64, error) {
	return c.connector.Connect(key)
}

// Dispose the connector.
func (c *ZKCluster) Shutdown() {
	c.Stop()

	if c.connector != nil {
		c.connector.Shutdown()
		c.connector = nil
	}
	if c.zc != nil {
		c.zc.Shutdown()
		c.zc = nil
	}
}

// ZK assisted NodeReader implementation.
func (c *ZKCluster) ReadNodes() []Shard {
	cluster_root := ZK_ROOT + "/" + c.info.Name
	if shardbytes, _, err := c.zc.conn.Get(cluster_root + "/shards"); err != nil {
		return nil
	} else {
		var shards []Shard
		if err := json.Unmarshal(shardbytes, &shards); err != nil {
			return nil
		}
		return shards
	}
}

// ZK assisted RingBuilder implementation.
// The ring type or the parameters might be designated via the zookeeper.
func (c *ZKCluster) BuildRing(shards []Shard) *HashRing {
	switch strings.ToLower(c.info.Options.RingType) {
	case "consistent":
		nreplica, err := strconv.Atoi(c.info.Options.RingParams)
		if err != nil {
			nreplica = len(shards)
		}
		ring := &ConsistentRing{
			Nreplica: nreplica,
		}
		return ring.BuildRing(shards)
	default:
		return nil
	}
}

// The watcher for status updates via the zookeeper.
func (c *ZKCluster) watchStatusUpdates() {
	status_node := ZK_ROOT + "/" + c.info.Name + "/status"
	for {
		exists, stat, event, err := c.zc.conn.ExistsW(status_node)
		if err == nil && exists && stat.Version > c.version {
			if statusbytes, stat, err := c.zc.conn.Get(status_node); err == nil {
				var status map[string]ShardStatus
				if err := json.Unmarshal(statusbytes, &status); err == nil {
					for k, v := range status {
						if c.status[k] != v {
							c.status[k] = v
							c.updates <- v
						}
					}
					c.version = stat.Version
				}
			}
		}
		select {
		case <- event:
		case <- c.checkerdone: return
		}
	}
}

// ZK assisted HealthChecker implementation.
func (c *ZKCluster) Start(shards []Shard) <-chan ShardStatus {
	if c.checkerdone != nil {
		return c.updates
	}

	c.checkerdone = make(chan bool)
	c.status = make(map[string]ShardStatus, len(shards))
	c.updates = make(chan ShardStatus, len(shards))
	go c.watchStatusUpdates()

	return c.updates

}

// ZK assisted HealthChecker implementation.
func (c *ZKCluster) Stop() {
	if c.checkerdone != nil {
		close(c.checkerdone)
		close(c.updates)
		c.checkerdone = nil
	}
}
