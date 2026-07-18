package ipc

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"syscall"
	"time"
)

// ErrAddressInUse reports that another server is already listening on the address.
var ErrAddressInUse = errors.New("address already in use")

// Server read and write timeouts.
const (
	defaultReadTimeout  = 10 * time.Second
	defaultWriteTimeout = 10 * time.Second
)

type SocketServer[T, R any] struct {
	listener     net.Listener
	wg           sync.WaitGroup
	handler      MessageHandler[T, R]
	ready        chan struct{}
	readTimeout  time.Duration
	writeTimeout time.Duration
}

// ServerOption configures optional server behaviour.
type ServerOption func(*serverConfig)

type serverConfig struct {
	readTimeout  time.Duration
	writeTimeout time.Duration
}

// WithReadTimeout sets the deadline for Read calls.
func WithReadTimeout(d time.Duration) ServerOption {
	return func(c *serverConfig) { c.readTimeout = d }
}

// WithWriteTimeout limits the response write.
func WithWriteTimeout(d time.Duration) ServerOption {
	return func(c *serverConfig) { c.writeTimeout = d }
}

// NewServer creates a server that owns a unix socket at address.
// The server will offload received messages to the handler for processing.
// Stale socket files will be reclaimed.
func NewServer[T, R any](
	address string,
	handler MessageHandler[T, R],
	opts ...ServerOption,
) (*SocketServer[T, R], error) {
	l, err := listenUnix(address)
	if err != nil {
		return nil, err
	}

	return NewServerWithListener(l, handler, opts...), nil
}

// NewServerWithListener serves on a provided listener.
//
// The caller must close the listener.
// Socket files not created by the server will not be removed.
func NewServerWithListener[T, R any](l net.Listener, handler MessageHandler[T, R], opts ...ServerOption) *SocketServer[T, R] {
	cfg := serverConfig{
		readTimeout:  defaultReadTimeout,
		writeTimeout: defaultWriteTimeout,
	}
	for _, opt := range opts {
		opt(&cfg)
	}

	return &SocketServer[T, R]{
		listener:     l,
		handler:      handler,
		ready:        make(chan struct{}),
		readTimeout:  cfg.readTimeout,
		writeTimeout: cfg.writeTimeout,
	}
}

// listenUnix binds a unix socket. Stale sockets are reclaimed.
// If a server is listening on it returns an ErrAddressInUse.
func listenUnix(address string) (net.Listener, error) {
	l, err := net.Listen("unix", address)
	if err == nil {
		return l, nil
	}
	if !errors.Is(err, syscall.EADDRINUSE) {
		return nil, fmt.Errorf("unix socket listen: %w", err)
	}

	if canDial(address) {
		return nil, fmt.Errorf("%w: %s", ErrAddressInUse, address)
	}

	// The socket file is stale. Remove and retry.
	_ = os.Remove(address)
	l, err = net.Listen("unix", address)
	if err != nil {
		return nil, fmt.Errorf("unix socket listen: %w", err)
	}
	return l, nil
}

// canDial reports whether a server is currently listening on the address.
func canDial(address string) bool {
	conn, err := net.DialTimeout("unix", address, 100*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// Run starts the server and blocks until the context is cancelled.
func (s *SocketServer[T, R]) Run(ctx context.Context) error {
	go s.listen(ctx)

	<-ctx.Done()
	s.stop()

	return nil
}

func (s *SocketServer[T, R]) stop() {
	_ = s.listener.Close()
	s.wg.Wait()
}

func (s *SocketServer[T, R]) listen(ctx context.Context) {
	close(s.ready)
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			select {
			case <-ctx.Done():
				return
			default:
				slog.Error("connection error", "err", err)
				continue
			}
		}

		s.wg.Add(1)
		go func(c net.Conn) {
			defer s.wg.Done()
			s.handle(c)
		}(conn)
	}
}

func (s *SocketServer[T, R]) handle(conn net.Conn) {
	defer func() {
		_ = conn.Close()
	}()

	_ = conn.SetReadDeadline(time.Now().Add(s.readTimeout))

	decoder := gob.NewDecoder(conn)
	var req T
	if err := decoder.Decode(&req); err != nil {
		slog.Error("message decode", "err", err)
		return
	}

	response := s.invoke(req)

	_ = conn.SetWriteDeadline(time.Now().Add(s.writeTimeout))

	if err := gob.NewEncoder(conn).Encode(response); err != nil {
		slog.Error("sending response to client", "err", err)
	}
}

// invoke calls the handler and returns a response.
//
// A handler panic is returned as an error response.
func (s *SocketServer[T, R]) invoke(req T) (response envelope[R]) {
	defer func() {
		if p := recover(); p != nil {
			slog.Error("handler panic", "panic", p)
			response = envelope[R]{ErrorMsg: fmt.Sprintf("handler panic: %v", p)}
		}
	}()

	result, err := s.handler.Handle(req)
	if err != nil {
		return envelope[R]{ErrorMsg: err.Error()}
	}

	return envelope[R]{Payload: result}
}
