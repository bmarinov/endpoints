// Package ipc provides typed request/response messaging between
// processes. A [Server] accepts connections on a [net.Listener], decodes one
// gob-encoded request per connection, invokes a [MessageHandler], and
// encodes back a single response. [Send] and [SendConn] are the client
// side of that exchange.
//
// The default transport is a unix socket ([NewServer], [Send]). The
// server and client both accept any [net.Conn] transport via
// [NewServerWithListener] and [SendConn].
package ipc

// MessageHandler defines an interface for processing a message of type T and returning a result of type R.
type MessageHandler[T, R any] interface {
	Handle(msg T) (R, error)
}

type HandlerFunc[T, R any] func(T) (R, error)

func (h HandlerFunc[T, R]) Handle(msg T) (R, error) {
	return h(msg)
}

// envelope is the internal message wrapper carrying a payload of type T
// and an optional error message indicating failure.
type envelope[T any] struct {
	Payload  T
	ErrorMsg string
}
