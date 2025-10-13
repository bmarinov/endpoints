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

var ErrServerNotRunning = errors.New("server not listening")

// Send sends a message of type T to the specified address and waits for a response of type R.
func Send[T, R any](ctx context.Context, msg T, address string) (R, error) {
	var result R

	conn, err := net.Dial("unix", address)
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

	encoder := gob.NewEncoder(conn)

	err = encoder.Encode(msg)
	if err != nil {
		return result, fmt.Errorf("sending command to server: %w", err)
	}

	decoder := gob.NewDecoder(conn)
	var response envelope[R]
	err = decoder.Decode(&response)
	if err != nil {
		return result, fmt.Errorf("server response read: %w", err)
	}

	return response.Payload, nil
}
