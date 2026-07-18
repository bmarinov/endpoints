package ipc

import (
	"context"
	"encoding/gob"
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"
)

var (
	ErrServerNotRunning = errors.New("server not listening")

	// ErrHandler reports that the call reached the server
	// but the handler itself failed.
	ErrHandler = errors.New("handler failed")
)

// DialFunc establishes a connection to a server. It is called once per Send.
type DialFunc func(context.Context) (net.Conn, error)

// Send sends a message of type T to a unix socket at address and waits for a response of type R.
func Send[T, R any](ctx context.Context, msg T, address string) (R, error) {
	return SendConn[T, R](ctx, msg, dialUnix(address))
}

// dialUnix dials a unix socket.
// Returns ErrServerNotRunning when the socket is missing or the connection refused.
func dialUnix(address string) DialFunc {
	return func(ctx context.Context) (net.Conn, error) {
		var dialer net.Dialer
		conn, err := dialer.DialContext(ctx, "unix", address)
		if err != nil {
			var syscallErr *os.SyscallError
			if errors.As(err, &syscallErr) && syscallErr.Syscall == "connect" {
				if errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ECONNREFUSED) {
					return nil, fmt.Errorf("%w: %w", ErrServerNotRunning, err)
				}
			}
			return nil, err
		}
		return conn, nil
	}
}

// SendConn sends a message of type T over a provided connection and waits
// for a response of type R.
// The connection may use any transport.
func SendConn[T, R any](ctx context.Context, msg T, dial DialFunc) (R, error) {
	var result R

	conn, err := dial(ctx)
	if err != nil {
		return result, fmt.Errorf("failed to connect: %w", err)
	}

	defer conn.Close()

	// Report ctx.Err() instead of the I/O error on cancel:
	done := make(chan struct{})
	defer close(done)
	go func() {
		select {
		case <-ctx.Done():
			_ = conn.Close()
		case <-done:
		}
	}()

	encoder := gob.NewEncoder(conn)

	err = encoder.Encode(msg)
	if err != nil {
		return result, callErr(ctx, fmt.Errorf("sending command to server: %w", err))
	}

	decoder := gob.NewDecoder(conn)
	var response envelope[R]
	err = decoder.Decode(&response)
	if err != nil {
		return result, callErr(ctx, fmt.Errorf("server response read: %w", err))
	}

	if response.ErrorMsg != "" {
		return result, fmt.Errorf("%w: %s", ErrHandler, response.ErrorMsg)
	}

	return response.Payload, nil
}

// callErr returns the context error if any, or err as fallback.
func callErr(ctx context.Context, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return fmt.Errorf("%w: %w", ctxErr, err)
	}
	return err
}
