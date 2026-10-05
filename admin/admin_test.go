package admin

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/Cyb3r-Jak3/go-grpc-proxy/client"
	"github.com/Cyb3r-Jak3/go-grpc-proxy/internal/testutil"
	"github.com/Cyb3r-Jak3/go-grpc-proxy/server"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func startServer(t *testing.T) (*server.Server, Config) {
	t.Helper()
	srv := server.New(quiet())
	opts, _ := testutil.Start(t, srv)
	return srv, Config{ServerAddr: "passthrough:///bufnet", Log: quiet(), DialOptions: opts}
}

func startAgent(t *testing.T, cfg Config, id string) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = client.Run(ctx, client.Config{
			AgentID: id, ServerAddr: cfg.ServerAddr, Interval: 10 * time.Millisecond,
			Log: quiet(), DialOptions: cfg.DialOptions,
		})
	}()
	t.Cleanup(func() { cancel(); <-done })
}

func TestRunRequiresAgentID(t *testing.T) {
	if err := Run(context.Background(), Config{}); err == nil {
		t.Fatal("expected error for empty agent id")
	}
}

func TestRunRelaysInput(t *testing.T) {
	_, cfg := startServer(t)
	startAgent(t, cfg, "agent-1")

	pr, pw := io.Pipe()
	out := &testutil.SyncBuffer{}
	cfg.AgentID, cfg.In, cfg.Out = "agent-1", pr, out

	done := make(chan error, 1)
	go func() { done <- Run(context.Background(), cfg) }()

	testutil.WaitForOutput(t, out, "Session established")
	if _, err := io.WriteString(pw, "hello\n"); err != nil {
		t.Fatal(err)
	}
	testutil.WaitForOutput(t, out, "agent> hello")

	// Closing stdin ends the session cleanly.
	pw.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after stdin EOF")
	}
	for _, want := range []string{`Requesting session with agent "agent-1"`, "[server] waiting", "[server] connected"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("output missing %q:\n%s", want, out.String())
		}
	}
}

func TestRunCancelledWhileWaiting(t *testing.T) {
	_, cfg := startServer(t) // no agent ever checks in
	out := &testutil.SyncBuffer{}
	cfg.AgentID, cfg.In, cfg.Out = "ghost", strings.NewReader(""), out

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- Run(ctx, cfg) }()
	testutil.WaitForOutput(t, out, "[server] waiting")
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestRunRejectedWhenBusy(t *testing.T) {
	_, cfg := startServer(t)

	// First admin holds the claim (agent never shows up).
	first := cfg
	first.AgentID, first.In, first.Out = "busy", strings.NewReader(""), &testutil.SyncBuffer{}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	firstOut := first.Out.(*testutil.SyncBuffer)
	go func() { _ = Run(ctx, first) }()
	testutil.WaitForOutput(t, firstOut, "[server] waiting")

	out := &testutil.SyncBuffer{}
	cfg.AgentID, cfg.In, cfg.Out = "busy", strings.NewReader(""), out
	err := Run(context.Background(), cfg)
	if err != errRejected {
		t.Fatalf("Run = %v, want errRejected", err)
	}
	if !strings.Contains(out.String(), "[server] rejected") {
		t.Errorf("output = %q", out.String())
	}
}

func TestListAgentsEmpty(t *testing.T) {
	_, cfg := startServer(t)
	out := &testutil.SyncBuffer{}
	cfg.Out = out
	if err := ListAgents(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "No agents known") {
		t.Fatalf("output = %q", out.String())
	}
}

func TestListAgentsTable(t *testing.T) {
	srv, cfg := startServer(t)

	startAgentWithMeta := func(id string, tags []string, desc string) {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() {
			defer close(done)
			_ = client.Run(ctx, client.Config{
				AgentID: id, ServerAddr: cfg.ServerAddr, Interval: time.Hour,
				Log: quiet(), DialOptions: cfg.DialOptions, Tags: tags, Description: desc,
			})
		}()
		t.Cleanup(func() { cancel(); <-done })
	}
	startAgentWithMeta("tagged", []string{"a", "b"}, "my laptop")
	startAgentWithMeta("bare", nil, "")
	testutil.WaitFor(t, "both agents", func() bool { return len(srv.Clients()) == 2 })

	out := &testutil.SyncBuffer{}
	cfg.Out = out
	if err := ListAgents(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	got := out.String()
	for _, want := range []string{"CLIENT ID", "tagged", "a,b", "my laptop", "bare", "IDLE", "no"} {
		if !strings.Contains(got, want) {
			t.Errorf("output missing %q:\n%s", want, got)
		}
	}
	// The bare agent shows dashes for empty tags and description.
	for _, line := range strings.Split(got, "\n") {
		if strings.HasPrefix(line, "bare") && strings.Count(line, " -") < 2 {
			t.Errorf("bare agent line should show '-' placeholders: %q", line)
		}
	}
}

func TestNormalizeErr(t *testing.T) {
	if normalizeErr(io.EOF) != nil {
		t.Error("io.EOF should normalize to nil")
	}
	if normalizeErr(errRejected) != errRejected {
		t.Error("other errors should pass through")
	}
}
