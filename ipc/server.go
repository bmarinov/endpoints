package ipc

import (
	"context"
	"encoding/gob"
	"fmt"
	"log/slog"
	"net"
	"os"
	"sync"
	"time"
)

// Server read and write timeouts.
const (
	defaultReadTimeout  = 10 * time.Second
	defaultWriteTimeout = 10 * time.Second
)

type SocketServer[T, R any] struct {
	addr         string
	listener     net.Listener
	wg           sync.WaitGroup
	handler      MessageHandler[T, R]
	ready        chan struct{}
	readTimeout  time.Duration
	writeTimeout time.Duration
}

// NewServer creates a new socket server instance listening on the provided address.
// The server will offload received messages to the handler for processing.
func NewServer[T, R any](address string, handler MessageHandler[T, R]) *SocketServer[T, R] {
	_ = os.Remove(address)

	l, err := net.Listen("unix", address)
	if err != nil {
		panic(fmt.Errorf("unix socket listen: %w", err))
	}

	return &SocketServer[T, R]{
		addr:         address,
		listener:     l,
		wg:           sync.WaitGroup{},
		handler:      handler,
		ready:        make(chan struct{}),
		readTimeout:  defaultReadTimeout,
		writeTimeout: defaultWriteTimeout,
	}
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
	_ = os.Remove(s.addr)
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
