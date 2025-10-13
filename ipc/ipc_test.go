package ipc

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestSend(t *testing.T) {
	t.Run("send receive", func(t *testing.T) {
		socketAddr := filepath.Join(os.TempDir(), "stest.sock")

		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()
		var wg sync.WaitGroup
		wg.Add(1)

		response := "pong"
		srv := NewServer(socketAddr, HandlerFunc[string, string](func(msg string) (string, error) {
			return msg + response, nil
		}))

		go func() {
			defer wg.Done()
			err := srv.Run(ctx)
			if err != nil {
				t.Error(err)
			}
		}()

		<-srv.ready
		request := "ping"
		result, err := Send[string, string](ctx, request, socketAddr)
		if err != nil {
			t.Error(err)
		}

		expected := request + response
		if expected != result {
			t.Errorf("expected '%s' got '%s'", response, result)
		}

		cancel()
		wg.Wait()
	})
	t.Run("server not running", func(t *testing.T) {
		socketAddr := filepath.Join(os.TempDir(), "not.existing.sock")

		_, err := Send[string, string](t.Context(), "hi", socketAddr)
		if err == nil {
			t.Fatal("expected error")
		}
		if !errors.Is(err, ErrServerNotRunning) {
			t.Errorf("expected sentinel error, got %v", err)
		}
	})

}
