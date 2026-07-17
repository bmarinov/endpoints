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

// Send sends a message of type T to the specified address and waits for a response of type R.
func Send[T, R any](ctx context.Context, msg T, address string) (R, error) {
	var result R

	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "unix", address)
	if err != nil {
		var syscallErr *os.SyscallError
		if errors.As(err, &syscallErr) && syscallErr.Syscall == "connect" {
			if errors.Is(err, syscall.ENOENT) || errors.Is(err, syscall.ECONNREFUSED) {
				return result, fmt.Errorf("%w: %w", ErrServerNotRunning, err)
			}
		}
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
