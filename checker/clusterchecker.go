package checker

import (
	"sync"
	"sync/atomic"
	"time"
	
	. "github.com/beatuslapis/gorelib.v0/connector"
	
	"github.com/mediocregopher/radix.v2/pool"
)

// Context informations for a shard
type checkContext struct {
	shard *Shard
	mpool *pool.Pool
	alive time.Time
	delay uint
	penalty uint
	incheck bool
}

// A simple checker implementation for clusterized redis instances
type ClusterChecker struct {
	Nworker int
	Interval time.Duration
	Threshold time.Duration

	checkerstatus int32

	statusfn StatusFunc
	jobs chan *checkContext
	done chan bool
	waitscheduler sync.WaitGroup
	waitmonitor sync.WaitGroup
}

// Status constants for the checker
const (
	checkerStopped = 0
	checkerStarted
)

// A scheduler function for each shard.
// Send a job to workers for each time period.
// It would skip a job when the check is in progress.
func (c *ClusterChecker) nodeScheduler(s *Shard) {
	cxt := &checkContext{
		shard: s,
		delay: 1,
		penalty: 2,
	}
	for {
		select {
		case <- c.done:
			return
		default:
			if !cxt.incheck {
				c.jobs <- cxt
			}
			time.Sleep(c.Interval)
		}
	}
}

// Process an alive event of a shard.
// If the shard has penalties, it would wait penalties decreasing.
// Each alive event would decrease penalties by 1.
func (c *ClusterChecker) processAlive(cxt *checkContext) {
	switch cxt.shard.GetStatus() {
	case StatusUnknown:
		c.statusfn(cxt.shard, StatusUp)
	case StatusDown:
		if cxt.delay == 1 {
			c.statusfn(cxt.shard, StatusUp)
		}
	case StatusUp:
	default:
	}

	cxt.alive = time.Now()
	if cxt.delay > 1 {
		cxt.delay--
	}
	if cxt.penalty > 1 {
		cxt.penalty--
	}
}

// Process a dead event of a shard.
func (c *ClusterChecker) processDead(cxt *checkContext) {
	switch cxt.shard.GetStatus() {
	case StatusUnknown:
		c.statusfn(cxt.shard, StatusDown)
	case StatusDown:
	case StatusUp:
		if time.Since(cxt.alive) > c.Threshold {
			cxt.delay = cxt.penalty
			cxt.penalty *= 3
			if maxpenalty := uint(c.Threshold.Seconds() * 10); cxt.penalty > maxpenalty {
				cxt.penalty = maxpenalty
			}
			c.statusfn(cxt.shard, StatusDown)
		}
	default:
	}
}

// Check a shard.
// Send a redis "PING" command and checks its response.
func (c *ClusterChecker) checkNode(cxt *checkContext) {
	cxt.incheck = true
	defer func() { cxt.incheck = false }()

	if cxt.mpool == nil {
		if pool, err := pool.New("tcp", cxt.shard.Addr, 1); err == nil {
			cxt.mpool = pool
		}
	}
	if cxt.mpool != nil {
		if client, err := cxt.mpool.Get(); err == nil {
			defer cxt.mpool.Put(client)
			
			resp := client.Cmd("PING")
			if result, err := resp.Str(); err == nil && result == "PONG" {
				c.processAlive(cxt)
				return
			}
		}
	}
	c.processDead(cxt)
}

// A worker function to monitor shards using the job channel.
func (c *ClusterChecker) nodeMonitor() {
	for cxt := range c.jobs {
		select {
		case <- c.done:
			return
		default:
			c.checkNode(cxt)
		}
	}
}

// Start the checker.
// If it is already running, do nothing.
func (c *ClusterChecker) Start(shards []Shard, setStatus StatusFunc) {
	if !atomic.CompareAndSwapInt32(&c.checkerstatus, checkerStopped, checkerStarted) {
		return
	}

	c.statusfn = setStatus
	c.jobs = make(chan *checkContext, len(shards))
	c.done = make(chan bool)
	
	c.waitscheduler.Add(len(shards))
	for i, _ := range shards {
		go func(idx int) {
			c.nodeScheduler(&shards[idx])
			c.waitscheduler.Done()
		}(i)
	}
	
	c.waitmonitor.Add(c.Nworker)
	for i := 0; i < c.Nworker; i++ {
		go func() {
			c.nodeMonitor()
			c.waitmonitor.Done()
		}()
	}
}

// Stop the checker.
// If it is already stopped, do nothing.
func (c *ClusterChecker) Stop() {
	if !atomic.CompareAndSwapInt32(&c.checkerstatus, checkerStarted, checkerStopped) {
		return
	}

	// close channels
	close(c.done)
	close(c.jobs)

	// wait to stop schedulers and monitors
	c.waitscheduler.Wait()
	c.waitmonitor.Wait()
}
