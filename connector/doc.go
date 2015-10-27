// Package connector is a collection of Connector implementations for the gorecache.
//
// The package includes following implementations
//
// single - A connector for a single redis instance with the radix pool.
// Just a simple wrapper.
//
// cluster - A connector for a clusterized redis instances.
// It uses the HashRing to distribute and locate data with keys.
// To build the HashRing, it requires NodeReader for cluster topologies,
// and RingBuilder to specify sharding and failover strategies.
//
package connector
