package ipc

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

// startServer runs a server on a fresh socket and returns its address.
// The server is stopped when the test ends.
func startServer[T, R any](t *testing.T, handler MessageHandler[T, R], opts ...func(*SocketServer[T, R])) string {
	t.Helper()

	addr := filepath.Join(t.TempDir(), "s.sock")
	ctx, cancel := context.WithCancel(t.Context())

	srv := NewServer(addr, handler)
	for _, opt := range opts {
		opt(srv)
	}

	var wg sync.WaitGroup
	wg.Go(func() {
		if err := srv.Run(ctx); err != nil {
			t.Error(err)
		}
	})

	t.Cleanup(func() {
		cancel()
		wg.Wait()
	})

	<-srv.ready
	return addr
}

func TestSend(t *testing.T) {
	t.Run("send receive", func(t *testing.T) {
		addr := startServer(t, HandlerFunc[string, string](func(msg string) (string, error) {
			return msg + "pong", nil
		}))

		result, err := Send[string, string](t.Context(), "ping", addr)
		if err != nil {
			t.Fatal(err)
		}

		expected := "pingpong"
		if expected != result {
			t.Errorf("expected '%s' got '%s'", expected, result)
		}
	})
	t.Run("server not running", func(t *testing.T) {
		socketAddr := filepath.Join(t.TempDir(), "not.existing.sock")

		_, err := Send[string, string](t.Context(), "hi", socketAddr)
		if err == nil {
			t.Fatal("expected error")
		}
		if !errors.Is(err, ErrServerNotRunning) {
			t.Errorf("expected sentinel error, got %v", err)
		}
	})

	t.Run("handler error", func(t *testing.T) {
		addr := startServer(t, HandlerFunc[string, string](func(string) (string, error) {
			return "", errors.New("plan is unrunnable")
		}))

		_, err := Send[string, string](t.Context(), "hi", addr)
		if err == nil {
			t.Fatal("expected handler error, got success")
		}
		if !errors.Is(err, ErrHandler) {
			t.Errorf("expected sentinel error, got %v", err)
		}
		if !strings.Contains(err.Error(), "plan is unrunnable") {
			t.Errorf("expected handler's message, got %v", err)
		}
	})

	t.Run("handler error discards payload", func(t *testing.T) {
		addr := startServer(t, HandlerFunc[string, string](func(string) (string, error) {
			return "leaked", errors.New("refused")
		}))

		result, err := Send[string, string](t.Context(), "hi", addr)
		if err == nil {
			t.Fatal("expected error")
		}
		if result != "" {
			t.Errorf("expected zero value when handler errors, got '%s'", result)
		}
	})

	t.Run("handler panic", func(t *testing.T) {
		addr := startServer(t, HandlerFunc[string, string](func(msg string) (string, error) {
			if msg == "boom" {
				panic("handler exploded")
			}
			return "ok", nil
		}))

		_, err := Send[string, string](t.Context(), "boom", addr)
		if err == nil {
			t.Fatal("expected error from panicking handler, got success")
		}
		if !errors.Is(err, ErrHandler) {
			t.Errorf("expected sentinel error, got %v", err)
		}
		if !strings.Contains(err.Error(), "handler exploded") {
			t.Errorf("expected panic value, got %v", err)
		}

		result, err := Send[string, string](t.Context(), "fine", addr)
		if err != nil {
			t.Fatalf("expected server to survive handler panic and keep serving, got %v", err)
		}
		if result != "ok" {
			t.Errorf("expected 'ok' got '%s'", result)
		}
	})

	t.Run("slow handler", func(t *testing.T) {
		// broken on v0.1.0 with the enforced 1s deadline on the whole exchange.
		addr := startServer(t, HandlerFunc[string, string](func(string) (string, error) {
			time.Sleep(1200 * time.Millisecond)
			return "slow but done", nil
		}))

		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		defer cancel()

		result, err := Send[string, string](ctx, "hi", addr)
		if err != nil {
			t.Fatalf("expected handler slower than the connection deadline to complete, got %v", err)
		}
		if result != "slow but done" {
			t.Errorf("expected 'slow but done' got '%s'", result)
		}
	})

	t.Run("caller deadline", func(t *testing.T) {
		addr := startServer(t, HandlerFunc[string, string](func(string) (string, error) {
			time.Sleep(2 * time.Second)
			return "too late", nil
		}))

		ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
		defer cancel()

		start := time.Now()
		_, err := Send[string, string](ctx, "hi", addr)
		if err == nil {
			t.Fatal("expected error, got success")
		}
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("expected caller's deadline to be reported, got %v", err)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Errorf("expected call to end at caller's deadline, took %v", elapsed)
		}
	})

	t.Run("caller cancellation", func(t *testing.T) {
		addr := startServer(t, HandlerFunc[string, string](func(string) (string, error) {
			time.Sleep(2 * time.Second)
			return "too late", nil
		}))

		ctx, cancel := context.WithCancel(t.Context())
		go func() {
			time.Sleep(100 * time.Millisecond)
			cancel()
		}()

		start := time.Now()
		_, err := Send[string, string](ctx, "hi", addr)
		if err == nil {
			t.Fatal("expected error, got success")
		}
		if !errors.Is(err, context.Canceled) {
			t.Errorf("expected cancellation to be reported, got %v", err)
		}
		if elapsed := time.Since(start); elapsed > time.Second {
			t.Errorf("expected call to end on cancellation, took %v", elapsed)
		}
	})
}

// captureLogs redirects the default logger for the duration of the test.
func captureLogs(t *testing.T) func() string {
	t.Helper()

	buf := &syncBuffer{}
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(buf, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })

	return buf.String
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func TestServer(t *testing.T) {
	t.Run("handler error logging", func(t *testing.T) {
		logs := captureLogs(t)

		addr := startServer(t, HandlerFunc[string, string](func(string) (string, error) {
			return "", errors.New("plan is unrunnable")
		}))

		if _, err := Send[string, string](t.Context(), "hi", addr); err == nil {
			t.Fatal("expected error")
		}

		if strings.Contains(logs(), "sending response to client") {
			t.Errorf("expected handler error not to be logged as a transport failure, got:\n%s", logs())
		}
	})

	t.Run("response timeout", func(t *testing.T) {
		addr := startServer(t,
			HandlerFunc[string, string](func(string) (string, error) {
				time.Sleep(200 * time.Millisecond)
				return "done", nil
			}),
			func(s *SocketServer[string, string]) { s.writeTimeout = 50 * time.Millisecond },
		)

		result, err := Send[string, string](t.Context(), "hi", addr)
		if err != nil {
			t.Fatalf("expected response timeout to cover only the response, not the handler, got %v", err)
		}
		if result != "done" {
			t.Errorf("expected 'done' got '%s'", result)
		}
	})

	t.Run("decode failure", func(t *testing.T) {
		var called bool
		var mu sync.Mutex

		addr := startServer(t, HandlerFunc[string, string](func(string) (string, error) {
			mu.Lock()
			called = true
			mu.Unlock()
			return "ok", nil
		}))

		conn, err := net.Dial("unix", addr)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()

		if _, err := conn.Write([]byte("this is not gob\n")); err != nil {
			t.Fatal(err)
		}
		if err := conn.(*net.UnixConn).CloseWrite(); err != nil {
			t.Fatal(err)
		}

		buf := make([]byte, 1)
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		if _, err := conn.Read(buf); err == nil {
			t.Error("expected no response to an undecodable request")
		}

		mu.Lock()
		defer mu.Unlock()
		if called {
			t.Error("expected undecodable request to skip the handler, ran it on a zero value")
		}
	})
}
