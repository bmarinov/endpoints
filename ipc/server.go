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

type SocketServer[T, R any] struct {
	addr     string
	listener net.Listener
	wg       sync.WaitGroup
	handler  MessageHandler[T, R]
	ready    chan struct{}
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
		addr:     address,
		listener: l,
		wg:       sync.WaitGroup{},
		handler:  handler,
		ready:    make(chan struct{}),
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
	defer conn.Close()
	conn.SetDeadline(time.Now().Add(1 * time.Second))

	decoder := gob.NewDecoder(conn)
	var req T
	err := decoder.Decode(&req)
	if err != nil {
		slog.Error("message decode", "err", err)
	}

	result, err := s.handler.Handle(req)

	var response envelope[R]
	encoder := gob.NewEncoder(conn)

	if err != nil {
		response = envelope[R]{ErrorMsg: err.Error()}
	} else {
		response = envelope[R]{Payload: result}
	}

	encoder.Encode(response)
	if err != nil {
		slog.Error("sending response to client", "err", err)
	}
}
