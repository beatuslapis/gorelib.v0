package zkcluster

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"

	. "github.com/beatuslapis/gorelib.v0/connector/cluster"

	"github.com/samuel/go-zookeeper/zk"
)

// Cluster options stored on the zookeeper.
type ZKClusterOptions struct {
	FailoverEnabled bool
	RingType string
	RingParams string
}

// Cluster information stored on the zookeeper.
type ZKClusterInfo struct {
	Name string
	Version int64
	Options ZKClusterOptions
	Shards []Shard
}

// Connector interface to zookeeper servers.
type ZKConnector struct {
	servers []string
	conn *zk.Conn
	events <-chan zk.Event
}

const (
	ZK_ROOT = "/goreclusters"
	DEF_FLAGS = int32(0)
	EPH_SEQ_FLAGS = zk.FlagEphemeral | zk.FlagSequence
)

var (
	DEF_ACL = zk.WorldACL(zk.PermAll)
)

func NewZKConnector(servers []string, timeout time.Duration) (*ZKConnector, error) {
	zr := &ZKConnector{
		servers: servers,
	}
	if conn, events, err := zk.Connect(servers, timeout); err != nil {
		return nil, err
	} else {
		zr.conn = conn
		zr.events = events
	}

	return zr, nil
}

func (zc *ZKConnector) GetCluster(name string) (*ZKClusterInfo, error) {
	c := &ZKClusterInfo{
		Name: name,
	}

	cluster_root := ZK_ROOT + "/" + name
	if cluster, _, err := zc.conn.Get(cluster_root); err != nil {
		return nil, err
	} else {
		c.Version, _ = strconv.ParseInt(string(cluster), 10, 64)
	}

	if shardbytes, _, err := zc.conn.Get(cluster_root + "/shards"); err != nil {
		return nil, err
	} else if err := json.Unmarshal(shardbytes, &c.Shards); err != nil {
		return nil, err
	}

	if optionbytes, _, err := zc.conn.Get(cluster_root + "/options"); err != nil {
		return nil, err
	} else if err := json.Unmarshal(optionbytes, &c.Options); err != nil {
		return nil, err
	}

	return c, nil
}

func (zc *ZKConnector) GetClusters() ([]*ZKClusterInfo, error) {
	cs := make([]*ZKClusterInfo, 0)

	if clusters, _, err := zc.conn.Children(ZK_ROOT); err != nil {
		return nil, err
	} else {
		for _, c := range clusters {
			if cluster, err := zc.GetCluster(c); err == nil {
				cs = append(cs, cluster)
			} else {
				fmt.Errorf("failed to get cluster:", c, " err:", err)
			}
		}
	}

	return cs, nil
}

func (zc *ZKConnector) Shutdown() {
	if zc.conn != nil {
		zc.conn.Close()
		zc.conn = nil
	}
}

