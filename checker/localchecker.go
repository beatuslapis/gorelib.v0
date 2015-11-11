package checker

import (
	"sync"
	"sync/atomic"
	"time"

	. "github.com/beatuslapis/gorelib.v0/connector/cluster"
	
	"github.com/mediocregopher/radix.v2/pool"
	"fmt"
)

// Context information for a shard
type checkContext struct {
	status ShardStatus
	mpool *pool.Pool
	lastalive time.Time
	delay uint
	penalty uint
	incheck bool
}

// A simple checker implementation for clustered redis instances
type LocalChecker struct {
	Nworker int
	Interval time.Duration
	Threshold time.Duration

	checkerstatus int32

	jobs chan *checkContext
	done chan bool
	updates chan ShardStatus
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
func (c *LocalChecker) nodeScheduler(s *Shard) {
	cxt := &checkContext{
		status: ShardStatus{
			Addr: s.Addr,
		},
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

func (c *LocalChecker) sendUpdate(cxt *checkContext, isAlive bool) {
	defer func(){
		s := recover()
		fmt.Errorf("sendUpdate recovered from [%s]", s)
	}()

	cxt.status.Alive = isAlive
	cxt.status.Since = time.Now().UnixNano() / 1000

	c.updates <- cxt.status
}

// Process an alive event of a shard.
// If the shard has penalties, it would wait penalties decreasing.
// Each alive event would decrease penalties by 1.
func (c *LocalChecker) processAlive(cxt *checkContext) {
	if (!cxt.status.Alive && cxt.delay == 1) || cxt.status.Since == 0 {
		c.sendUpdate(cxt, true)
	}

	cxt.lastalive = time.Now()
	if cxt.delay > 1 {
		cxt.delay--
	}
	if cxt.penalty > 1 {
		cxt.penalty--
	}
}

// Process a dead event of a shard.
func (c *LocalChecker) processDead(cxt *checkContext) {
	if (cxt.status.Alive && time.Since(cxt.lastalive) > c.Threshold) || cxt.status.Since == 0 {
		cxt.delay = cxt.penalty
		cxt.penalty *= 3
		if maxpenalty := uint(c.Threshold.Seconds() * 10); cxt.penalty > maxpenalty {
			cxt.penalty = maxpenalty
		}
		c.sendUpdate(cxt, false)
	}
}

// Check a shard.
// Send a redis "PING" command and checks its response.
func (c *LocalChecker) checkNode(cxt *checkContext) {
	cxt.incheck = true
	defer func() { cxt.incheck = false }()

	if cxt.mpool == nil {
		if pool, err := pool.New("tcp", cxt.status.Addr, 1); err == nil {
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
func (c *LocalChecker) nodeMonitor() {
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
func (c *LocalChecker) Start(shards []Shard) <-chan ShardStatus {
	if !atomic.CompareAndSwapInt32(&c.checkerstatus, checkerStopped, checkerStarted) {
		return nil
	}

	c.jobs = make(chan *checkContext, len(shards))
	c.done = make(chan bool)
	c.updates = make(chan ShardStatus, len(shards))
	
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

	return c.updates
}

// Stop the checker.
// If it is already stopped, do nothing.
func (c *LocalChecker) Stop() {
	if !atomic.CompareAndSwapInt32(&c.checkerstatus, checkerStarted, checkerStopped) {
		return
	}

	// close channels
	close(c.done)
	c.waitscheduler.Wait()

	close(c.jobs)
	c.waitmonitor.Wait()

	close(c.updates)
}
