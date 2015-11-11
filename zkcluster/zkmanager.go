package zkcluster
import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

type ZKManager struct {
	zc *ZKConnector
}

func NewZKManager(servers []string, timeout time.Duration) (*ZKManager, error) {
	zm := &ZKManager{}
	if zk, err := NewZKConnector(servers, timeout); err != nil {
		return nil, err
	} else {
		zm.zc = zk
	}

	if exists, _, err := zm.zc.conn.Exists(ZK_ROOT); err == nil && !exists {
		zm.zc.conn.Create(ZK_ROOT, []byte("goreclusters"), DEF_FLAGS, DEF_ACL)
	}

	return zm, nil
}

func (zm *ZKManager) CreateCluster(cluster *ZKClusterInfo) error {
	if cluster == nil {
		return errors.New("invalid cluster informations")
	}

	cluster_root := ZK_ROOT + "/" + cluster.Name
	if exists, _, err := zm.zc.conn.Exists(cluster_root); err != nil || exists {
		return errors.New("failed to check the existence of the cluster")
	}

	ctime := make([]byte, 0)
	ctime = strconv.AppendInt(ctime, time.Now().UnixNano() / 1000, 10)

	shardbytes, err := json.Marshal(cluster.Shards)
	if err != nil {
		return err
	}

	optionbytes, err := json.Marshal(cluster.Options)
	if err != nil {
		return err
	}

	var ret error = nil
	if _, err := zm.zc.conn.Create(cluster_root, ctime, DEF_FLAGS, DEF_ACL); err == nil {
		if _, err := zm.zc.conn.Create(cluster_root + "/shards", shardbytes, DEF_FLAGS, DEF_ACL); err == nil {
			if _, err := zm.zc.conn.Create(cluster_root + "/options", optionbytes, DEF_FLAGS, DEF_ACL); err == nil {
				return ret
			} else {
				ret = err
			}
			zm.zc.conn.Delete(cluster_root + "/shards", -1)
		} else {
			ret = err
		}
		zm.zc.conn.Delete(cluster_root, -1)
	} else {
		ret = err
	}

	return ret
}

func (zm *ZKManager) UpdateCluster(cluster *ZKClusterInfo) error {
	if cluster == nil {
		return errors.New("invalid cluster informations")
	}

	cluster_root := ZK_ROOT + "/" + cluster.Name
	if exists, _, err := zm.zc.conn.Exists(cluster_root); err != nil || !exists {
		return errors.New("failed to check the existence of the cluster")
	}

	mtime := make([]byte, 0)
	mtime = strconv.AppendInt(mtime, time.Now().UnixNano() / 1000, 10)

	shardbytes, err := json.Marshal(cluster.Shards)
	if err != nil {
		return err
	}

	optionbytes, err := json.Marshal(cluster.Options)
	if err != nil {
		return err
	}

	if _, err := zm.zc.conn.Set(cluster_root + "/shards", shardbytes, -1); err == nil {
		if _, err := zm.zc.conn.Set(cluster_root + "/options", optionbytes, -1); err == nil {
			if _, err := zm.zc.conn.Set(cluster_root, mtime, -1); err == nil {
				return nil
			} else {
				return err
			}
		} else {
			return err
		}
	} else {
		return err
	}
}

func (zm *ZKManager) DeleteCluster(name string) error {
	cluster_root := ZK_ROOT + "/" + name
	if exists, _, err := zm.zc.conn.Exists(cluster_root); err != nil || !exists {
		return errors.New("failed to check the existence of the cluster")
	}

	if nodes, _, err := zm.zc.conn.Children(cluster_root); err == nil {
		for _, node := range nodes {
			if err := zm.zc.conn.Delete(cluster_root + "/" + node, -1); err != nil {
				fmt.Errorf("failed to delete a node:", node, " err:", err)
			}
		}
	}

	if err := zm.zc.conn.Delete(cluster_root, -1); err != nil {
		return err
	}

	return nil
}

func (zm *ZKManager) Shutdown() {
	if zm.zc != nil {
		zm.zc.Shutdown()
	}
}