// Package push implements an optional real-time notification layer on top
// of the durable SQLite message journal (Solution B).
//
// The push server listens on a separate Unix socket and pushes lightweight
// message-published notifications to subscribers. It is purely a
// notification layer: the SQLite journal remains the source of truth, and
// subscribers recover missed messages via the pull-based messages.by_run
// RPC using the message_seq cursor carried in each notification.
//
// The Pusher interface is defined at the consumer side (internal/rpc) and
// implemented here. When push is disabled, a NoopPusher is used so the
// service behaves exactly as before.
package push

// Pusher is the consumer-side interface used by the RPC service to trigger
// a real-time push notification after a message is durably published to the
// SQLite journal. Implementations must be safe for concurrent use and must
// not block the caller on slow subscribers.
//
// NotifyMessage is best-effort: a failed or dropped notification does not
// undo the publish. Subscribers recover missed notifications via the
// pull-based messages.by_run RPC using the message_seq cursor.
type Pusher interface {
	NotifyMessage(runID string, messageSeq int64, eventID string)
}

// NoopPusher is a Pusher that does nothing. It is the default when the push
// server is disabled (no -push-socket), so the system works exactly as
// before with pull-based cursor polling.
type NoopPusher struct{}

// NotifyMessage implements Pusher by discarding the notification.
func (n *NoopPusher) NotifyMessage(string, int64, string) {}
