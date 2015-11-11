# GoReLib

[![GoDoc](https://godoc.org/github.com/beatuslapis/gorelib.v0?status.svg)](https://godoc.org/github.com/beatuslapis/gorelib.v0)

Collection of minimalistic libraries for golang using, hopefully clustered, redis servers.

* [cache](http://godoc.org/github.com/beatuslapis/gorelib.v0/cache) -
  A cache implementation

 * Supports an interface{} type for a cache key and value.
   Marshal/Unmarshal would be performed when needed.
   You may override them with your own marshal functions.

 * Provides a serial of the value with a form of an unix timestamp in millis.
   It could be used for validity, and possibly consistent, checks when using clustered connectors.
   Redis nodes on clusterized environments could go on and off inadvertently.
   Having serials could be useful on automatic cache invalidation.

 * Provides CheckAndSet method for more consistent CAS update patterns.
   When a long-taken or complex update needed,
   you could consider CAS patterns for the transaction using serial values.

* [connector](http://godoc.org/github.com/beatuslapis/gorelib.v0/connector) -
  A collection of connector implementations for the gorelib

 * [zkcluster](http://godoc.org/github.com/beatuslapis/gorelib.v0/zkcluster) -
   The connector and relevant components for the zookeeper assisted redis cluster.
   The connector should read cluster information and its shard status from the zookeeper.
   Separate checkers would update shard status in the zookeeper.
   Multiple checkers which would do quorum-like decisions on status changes are also possible.

* [checker](http://godoc.org/github.com/beatuslapis/gorelib.v0/checker) -
  A checker implementations for the clustered redis instances

## Features

* Use [radix.v2](https://github.com/mediocregopher/radix.v2) as connectors.
  You may use any connector you want which is compatible with the client of the radix.

## Installation

    go get github.com/beatuslapis/gorelib.v0/...

## Testing

    go test github.com/beatuslapis/gorelib.v0/...

The test action assumes you have the following running:

* A redis server listening on port 6379

## Copyright and licensing

Unless otherwise noted, the source files are distributed under the *MIT License*
found in the LICENSE.txt file.

[redis]: http://redis.io
[radix.v2]: https://github.com/mediocregopher/radix.v2
