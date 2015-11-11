// Checker implementations for a cluster connector
//
// localchecker - Check status of clustered redis instances.
// It checks responses of the redis 'PING' command.
// If a instance suffers status flapping, exponential backoff penalty would be applied.
// (However, the penalty would not be more than 10 times of dead detection threshold)
//
package checker