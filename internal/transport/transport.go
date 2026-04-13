package transport

import "context"

// Transport abstracts the WebRTC data channel transport.
// Tunnel code uses this interface exclusively — no direct jazz imports.
type Transport interface {
	// Connect establishes the transport connection.
	// Blocks until the data channel is open or ctx is cancelled.
	Connect(ctx context.Context) error

	// Send sends data through the data channel.
	Send(data []byte) error

	// Close tears down the transport.
	Close() error

	// Ready returns a channel that closes when the data channel is open and ready.
	Ready() <-chan struct{}

	// Done returns a channel that closes when the transport disconnects.
	Done() <-chan struct{}

	// CanSend reports whether the transport can accept more data (backpressure check).
	CanSend() bool

	// BufferedAmount reports the current buffered byte count of the best available send path.
	BufferedAmount() uint64

	// SetOnData registers a callback for incoming data.
	SetOnData(func([]byte))

	// SetOnReconnect registers a callback invoked after a successful reconnect.
	SetOnReconnect(func())
}
