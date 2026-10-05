package client_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/Cyb3r-Jak3/go-grpc-proxy/client"
	mdmv1 "github.com/Cyb3r-Jak3/go-grpc-proxy/gen/mdm/v1"
	"github.com/Cyb3r-Jak3/go-grpc-proxy/internal/testutil"
	"github.com/Cyb3r-Jak3/go-grpc-proxy/server"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type harness struct {
	srv   *server.Server
	admin mdmv1.AdminServiceClient
	cfg   client.Config
}

func newHarness(t *testing.T) harness {
	t.Helper()
	srv := server.New(quiet())
	opts, conn := testutil.Start(t, srv)
	return harness{
		srv:   srv,
		admin: mdmv1.NewAdminServiceClient(conn),
		cfg: client.Config{
			AgentID:     "agent-1",
			ServerAddr:  "passthrough:///bufnet",
			Interval:    10 * time.Millisecond,
			Log:         quiet(),
			Tags:        []string{"lab"},
			Description: "unit test",
			DialOptions: opts,
		},
	}
}

// start runs the agent and returns a function that stops it and waits.
func (h harness) start(t *testing.T) (stop func()) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx, h.cfg) }()
	var once sync.Once
	var wait func()
	stop = func() {
		once.Do(func() { cancel(); wait() })
	}
	wait = func() {
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run returned %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Error("Run did not return after cancel")
		}
	}
	t.Cleanup(stop)
	return stop
}

func (h harness) agentState() (mdmv1.ClientStatus, bool) {
	for _, c := range h.srv.Clients() {
		if c.ID == h.cfg.AgentID {
			return c.Status, true
		}
	}
	return 0, false
}

// connect attaches an admin and waits until the agent's stream is live.
func (h harness) connect(t *testing.T) mdmv1.AdminService_SessionClient {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	st, err := h.admin.Session(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = st.Send(&mdmv1.AdminFrame{Payload: &mdmv1.AdminFrame_Attach{Attach: &mdmv1.AdminAttach{AgentId: h.cfg.AgentID}}})
	for {
		f, err := st.Recv()
		if err != nil {
			t.Fatalf("waiting for connect: %v", err)
		}
		if f.GetEvent().GetState() == mdmv1.SessionState_SESSION_STATE_CONNECTED {
			return st
		}
	}
}

func roundTrip(t *testing.T, st mdmv1.AdminService_SessionClient, msg string) string {
	t.Helper()
	err := st.Send(&mdmv1.AdminFrame{Payload: &mdmv1.AdminFrame_Data{Data: []byte(msg)}})
	if err != nil {
		t.Fatal(err)
	}
	f, err := st.Recv()
	if err != nil {
		t.Fatal(err)
	}
	return string(f.GetData())
}

func TestRunCheckInsAndDisconnects(t *testing.T) {
	h := newHarness(t)
	stop := h.start(t)

	testutil.WaitFor(t, "agent registration", func() bool {
		s, ok := h.agentState()
		return ok && s == mdmv1.ClientStatus_CLIENT_STATUS_IDLE
	})
	if c := h.srv.Clients()[0]; c.Description != "unit test" || len(c.Tags) != 1 || c.Tags[0] != "lab" {
		t.Errorf("metadata not reported: %+v", c)
	}

	stop()
	if s, _ := h.agentState(); s != mdmv1.ClientStatus_CLIENT_STATUS_OFFLINE {
		t.Fatalf("status after shutdown = %v, want OFFLINE", s)
	}
}

func TestEchoSession(t *testing.T) {
	h := newHarness(t)
	h.start(t)
	st := h.connect(t)

	for _, msg := range []string{"hello", "world"} {
		if got := roundTrip(t, st, msg); got != msg {
			t.Fatalf("echo = %q, want %q", got, msg)
		}
	}

	testutil.WaitFor(t, "BUSY status", func() bool {
		s, _ := h.agentState()
		return s == mdmv1.ClientStatus_CLIENT_STATUS_BUSY
	})

	// Admin leaves; the agent returns to idle and can be claimed again.
	_ = st.CloseSend()
	testutil.WaitFor(t, "IDLE after session", func() bool {
		s, _ := h.agentState()
		return s == mdmv1.ClientStatus_CLIENT_STATUS_IDLE
	})
	st2 := h.connect(t)
	if got := roundTrip(t, st2, "again"); got != "again" {
		t.Fatalf("second session echo = %q", got)
	}
}

func TestCustomSessionHandler(t *testing.T) {
	h := newHarness(t)
	gotID := make(chan string, 1)
	h.cfg.SessionHandler = func(_ context.Context, s *client.Session) error {
		gotID <- s.ID
		for {
			data, err := s.Recv()
			if err != nil {
				return err
			}
			if err := s.Send(append([]byte("up:"), data...)); err != nil {
				return err
			}
		}
	}
	h.start(t)
	st := h.connect(t)

	if got := roundTrip(t, st, "abc"); got != "up:abc" {
		t.Fatalf("got %q", got)
	}
	select {
	case id := <-gotID:
		if id == "" {
			t.Error("handler saw empty session ID")
		}
	case <-time.After(time.Second):
		t.Fatal("handler never invoked")
	}
}

func TestHandlerReturningEndsSession(t *testing.T) {
	h := newHarness(t)
	h.cfg.SessionHandler = func(context.Context, *client.Session) error { return nil }
	h.start(t)
	st := h.connect(t)

	for {
		f, err := st.Recv()
		if err != nil {
			t.Fatalf("stream error before ENDED: %v", err)
		}
		if f.GetEvent().GetState() == mdmv1.SessionState_SESSION_STATE_ENDED {
			return
		}
	}
}

func TestRunSurvivesUnreachableServer(t *testing.T) {
	cfg := client.Config{
		AgentID:    "x",
		ServerAddr: "127.0.0.1:1", // nothing listening
		Interval:   10 * time.Millisecond,
		Log:        quiet(),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- client.Run(ctx, cfg) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, want nil on ctx cancel", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return")
	}
}
