package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDoGetAndReadResponse(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		host, closeFn := startLocalHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"ok":true}`))
		}))
		defer closeFn()

		ok := doGetAndReadResponse("test", host, "/", "GET", func(resp *http.Response) error {
			return nil
		})
		if !ok {
			t.Fatalf("expected request to succeed")
		}
	})

	t.Run("decode error", func(t *testing.T) {
		host, closeFn := startLocalHTTPServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
		}))
		defer closeFn()

		ok := doGetAndReadResponse("test", host, "/", "GET", func(resp *http.Response) error {
			return errors.New("decode failed")
		})
		if ok {
			t.Fatalf("expected request to fail when decode fails")
		}
	})
}

func TestResetDebugFlag(t *testing.T) {
	s := &PeerServer{debugFlag: DEBUG_FLAG_FWD | DEBUG_FLAG_JOIN | DEBUG_FLAG_STATUS}
	s.ResetDebugFlag()
	if s.debugFlag != 0 {
		t.Fatalf("expected debug flag to be reset, got %d", s.debugFlag)
	}
}

func startLocalHTTPServer(t *testing.T, handler http.Handler) (string, func()) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}

	srv := &http.Server{Handler: handler}
	go func() {
		_ = srv.Serve(l)
	}()

	return l.Addr().String(), func() {
		_ = srv.Close()
		_ = l.Close()
	}
}

func TestConsoleRunScriptIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test in short mode")
	}

	repoRoot := filepath.Clean(filepath.Join(".", ".."))
	runFileName := fmt.Sprintf("test/integration_%d.run", time.Now().UnixNano())
	runFilePath := filepath.Join(repoRoot, runFileName)
	script := strings.Join([]string{
		"s 1",
		"m 0 hello-from-run-script",
		"l 0",
		"k 0",
	}, "\n") + "\n"

	if err := os.WriteFile(runFilePath, []byte(script), 0o644); err != nil {
		t.Fatalf("write run script: %v", err)
	}
	defer os.Remove(runFilePath)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "go", "run", "./grapevine", "-app", "test", "-inproc")
	cmd.Dir = repoRoot
	cmd.Stdin = strings.NewReader("r " + runFileName + "\nq\n")
	out, err := cmd.CombinedOutput()
	output := string(out)

	if ctx.Err() == context.DeadlineExceeded {
		t.Fatalf("console integration timed out. output:\n%s", output)
	}
	if err == nil {
		t.Fatalf("expected non-zero exit (console uses log.Fatal on quit), got success. output:\n%s", output)
	}
	if !strings.Contains(output, "Started master peer") {
		t.Fatalf("expected console output to include startup message. output:\n%s", output)
	}
	if !strings.Contains(output, "latest") {
		t.Fatalf("expected console output to include last-message output. output:\n%s", output)
	}
}
