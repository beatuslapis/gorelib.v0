package zkcluster

import (
	"encoding/json"
	"errors"
	"path/filepath"
	"sort"
	"time"

	. "github.com/beatuslapis/gorelib.v0/checker"

	"github.com/samuel/go-zookeeper/zk"
)

type ZKCheckerOptions struct {
	ZKServers []string
	ZKTimeout time.Duration
	Clustername string
	Nworker int
	Interval time.Duration
	Threshold time.Duration
}

type ZKChecker struct {
	zc *ZKConnector
	name string

	id string
	info *ZKClusterInfo
	status map[string]ShardStatus

	done chan bool
	checker *LocalChecker
}

func NewZKChecker(options *ZKCheckerOptions) (*ZKChecker, error) {
	checker := &ZKChecker{
		checker: &LocalChecker{
			Nworker: options.Nworker,
			Interval: options.Interval,
			Threshold: options.Threshold,
		},
		status: make(map[string]ShardStatus),
	}

	if zc, err := NewZKConnector(options.ZKServers, options.ZKTimeout); err != nil {
		return nil, err
	} else {
		checker.zc = zc
	}
	checker.name = options.Clustername

	return checker, nil
}


func (c *ZKChecker) registerChecker() (string, error) {
	status_global := ZK_ROOT + "/" + c.info.Name + "/status"
	status_path := ZK_ROOT + "/" + c.info.Name + "/localstatus"

	// get global status
	if statusbytes, _, err := c.zc.conn.Get(status_global); err == nil {
		json.Unmarshal(statusbytes, &c.status)
	}

	// check local status node
	if exists, _, _ := c.zc.conn.Exists(status_path); !exists {
		c.zc.conn.Create(status_path, []byte(c.info.Name), DEF_FLAGS, DEF_ACL)
	}

	// create ephemeral sequential node
	statusbytes, _ := json.Marshal(c.status)
	if mynode, err := c.zc.conn.Create(status_path + "/n_", statusbytes, EPH_SEQ_FLAGS, DEF_ACL); err != nil {
		return "", err
	} else {
		return filepath.Base(mynode), nil
	}
}

func (c *ZKChecker) checkVotes() {
	status_global := ZK_ROOT + "/" + c.info.Name + "/status"
	globalstatus := make(map[string]ShardStatus)
	if globalbytes, _, err := c.zc.conn.Get(status_global); err == nil {
		json.Unmarshal(globalbytes, &globalstatus)
	}

	status_path := ZK_ROOT + "/" + c.info.Name + "/localstatus"
	changed := false
	if voters, _, err := c.zc.conn.Children(status_path); err == nil {
		ballotbox := make(map[string]int)
		for _, voter := range voters {
			if vote, _, err := c.zc.conn.Get(status_path + "/" + voter); err == nil {
				var shardstatus map[string]ShardStatus
				if err := json.Unmarshal(vote, &shardstatus); err == nil {
					for k, v := range shardstatus {
						if v.Alive {
							ballotbox[k]++
						} else {
							ballotbox[k]--
						}
					}
				}
			}
		}

		for k, v := range ballotbox {
			if v != 0 {
				if status, ok := globalstatus[k]; !ok || status.Alive != (v > 0) {
					globalstatus[k] = ShardStatus{
						Addr: k,
						Alive: v > 0,
						Since: time.Now().UnixNano() / 1000,
					}
					changed = true
				}
			}
		}
	}

	if changed {
		if statusbytes, err := json.Marshal(globalstatus); err == nil {
			if _, err := c.zc.conn.Set(status_global, statusbytes, -1); err == zk.ErrNoNode {
				c.zc.conn.Create(status_global, statusbytes, DEF_FLAGS, DEF_ACL)
			}
		}
	}
}

func (c *ZKChecker) statusUpdater() {
	status_path := ZK_ROOT + "/" + c.info.Name + "/localstatus"
	for {
		nodes, _, event, err := c.zc.conn.ChildrenW(status_path)
		if err == nil && len(nodes) > 0 {
			sort.Strings(nodes)
			if nodes[0] == c.id {
				for {
					exists, _, leaderevent, err := c.zc.conn.ExistsW(status_path)
					if err == nil && exists {
						c.checkVotes()
					}
					select {
					case <- leaderevent:
					case <- c.done: return
					}
				}
			}
		}
		select {
		case <- event:
		case <- c.done: return
		}
	}
}

func (c *ZKChecker) writeStatus() {
	status_path := ZK_ROOT + "/" + c.info.Name + "/localstatus"
	node_path := status_path + "/" + c.id
	if statusbytes, err := json.Marshal(c.status); err == nil {
		if _, err := c.zc.conn.Set(node_path, statusbytes, -1); err == nil {
			c.zc.conn.Set(status_path, []byte(c.info.Name), -1)
		}
	}
}

func (c *ZKChecker) watchUpdates(updates <-chan ShardStatus) {
	for update := range updates {
		if stat, ok := c.status[update.Addr]; !ok || stat.Alive != update.Alive {
			c.status[update.Addr] = update
			c.writeStatus()
		}
	}
}

func (c *ZKChecker) Start() error {
	if c.done != nil {
		return errors.New("Checker is already started.")
	}

	if info, err := c.zc.GetCluster(c.name); err != nil {
		return err
	} else {
		c.info = info
	}

	if id, err := c.registerChecker(); err != nil {
		return err
	} else {
		c.id = id
	}

	c.done = make(chan bool)
	go c.statusUpdater()

	updates := c.checker.Start(c.info.Shards)
	go c.watchUpdates(updates)

	return nil
}

func (c *ZKChecker) Stop() {
	if c.done != nil {
		close(c.done)
		c.done = nil

		c.checker.Stop()
	}
}

func (c *ZKChecker) Shutdown() {
	c.Stop()
	c.zc.Shutdown()
}
