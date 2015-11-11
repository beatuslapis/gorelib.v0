// Package connector is a collection of Connector implementations for the gorelib.
//
// The package includes following implementations
//
// single - A connector for a single redis instance with the radix pool.
// Just a simple wrapper.
//
// cluster - A connector for a clustered redis instances.
// It uses the HashRing to distribute and locate data with keys.
// To build the HashRing, it requires NodeReader for cluster topologies,
// and RingBuilder to specify shard and failover strategies.
//
// zkcluster - A connector for a zookeeper assisted redis cluster.
// It reads cluster configurations and shard status from zookeeper servers.
// It has a checker implementation which could be run separately.
// Multiple checkers are also possible
// and they would automatically elect a leader and make quorum-like decisions.
//
package connector
