package ipc_test

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"time"

	"github.com/bmarinov/endpoints/ipc"
)

// This is the complete round trip.
// Start a server, then send a message and read the response.
func ExampleNewServer() {
	handler := ipc.HandlerFunc[string, string](func(msg string) (string, error) {
		return "pong", nil
	})

	const addr = "/tmp/foobarsock"
	srv, err := ipc.NewServer(
		addr,
		handler,
		ipc.WithReadTimeout(3*time.Second),
		ipc.WithWriteTimeout(4*time.Second),
	)
	if err != nil {
		slog.Error("new server", "err", err)
		os.Exit(1)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		_ = srv.Run(ctx)
		close(done)
	}()

	response, err := ipc.Send[string, string](ctx, "ping", addr)
	if err != nil {
		slog.Error("ipc Send", "err", err)
		os.Exit(1)
	}

	slog.Info("received response", "message", response)

	cancel()
	<-done
}

// ipc.Send assumes that a server is already listening on addr.
func ExampleSend() {
	const addr = "/tmp/foobarsock"

	response, err := ipc.Send[string, string](context.Background(), "ping", addr)
	if err != nil {
		slog.Error("ipc Send", "err", err)
		os.Exit(1)
	}

	slog.Info("received response", "message", response)
}

// Send over a transport other than the default unix socket dialer.
func ExampleSendConn() {
	handler := ipc.HandlerFunc[string, string](func(msg string) (string, error) {
		return "pong", nil
	})

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		slog.Error("listen", "err", err)
		os.Exit(1)
	}
	srv := ipc.NewServerWithListener(l, handler)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		_ = srv.Run(ctx)
		close(done)
	}()

	dial := func(ctx context.Context) (net.Conn, error) {
		var d net.Dialer
		return d.DialContext(ctx, "tcp", l.Addr().String())
	}

	response, err := ipc.SendConn[string, string](ctx, "ping", dial)
	if err != nil {
		slog.Error("ipc SendConn", "err", err)
		os.Exit(1)
	}

	fmt.Println(response)

	cancel()
	<-done

	// Output:
	// pong
}
