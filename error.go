/*

Package kafka a provides high level client API for Apache Kafka.

Use 'Broker' for node connection management, 'Producer' for sending messages,
and 'Consumer' for fetching. All those structures implement Client, Consumer
and Producer interface, that is also implemented in kafkatest package.

*/
package kafka

import "context"

// isCanceled returns true when the given error is
// a context canceled or context deadline exceeded error.
func isCanceled(err error) bool {
	return err == context.Canceled || err == context.DeadlineExceeded
}
