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
