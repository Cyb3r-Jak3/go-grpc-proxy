package server_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	mdmv1 "github.com/Cyb3r-Jak3/go-grpc-proxy/gen/mdm/v1"
	"github.com/Cyb3r-Jak3/go-grpc-proxy/internal/testutil"
	"github.com/Cyb3r-Jak3/go-grpc-proxy/server"
	"github.com/Cyb3r-Jak3/go-grpc-proxy/store"
	"github.com/Cyb3r-Jak3/go-grpc-proxy/store/memory"
)

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

type env struct {
	srv   *server.Server
	agent mdmv1.AgentServiceClient
	admin mdmv1.AdminServiceClient
}

func setup(t *testing.T, opts ...server.Option) env {
	t.Helper()
	srv := server.New(quiet(), opts...)
	_, conn := testutil.Start(t, srv)
	return env{srv, mdmv1.NewAgentServiceClient(conn), mdmv1.NewAdminServiceClient(conn)}
}

func ctxT(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func wantCode(t *testing.T, err error, code codes.Code) {
	t.Helper()
	if got := status.Code(err); got != code {
		t.Fatalf("code = %v (err=%v), want %v", got, err, code)
	}
}

// attach opens an admin session targeting agentID and returns the stream.
func attach(t *testing.T, e env, agentID string) mdmv1.AdminService_SessionClient {
	t.Helper()
	st, err := e.admin.Session(ctxT(t))
	if err != nil {
		t.Fatal(err)
	}
	err = st.Send(&mdmv1.AdminFrame{Payload: &mdmv1.AdminFrame_Attach{Attach: &mdmv1.AdminAttach{AgentId: agentID}}})
	if err != nil {
		t.Fatal(err)
	}
	return st
}

func wantEvent(t *testing.T, st mdmv1.AdminService_SessionClient, state mdmv1.SessionState) *mdmv1.SessionEvent {
	t.Helper()
	f, err := st.Recv()
	if err != nil {
		t.Fatalf("recv (want %v): %v", state, err)
	}
	ev := f.GetEvent()
	if ev == nil || ev.GetState() != state {
		t.Fatalf("frame = %v, want event %v", f, state)
	}
	return ev
}

func TestNewDefaults(t *testing.T) {
	if server.New(nil) == nil {
		t.Fatal("New(nil) returned nil")
	}
}

func TestCheckInValidation(t *testing.T) {
	e := setup(t)
	_, err := e.agent.CheckIn(ctxT(t), &mdmv1.CheckInRequest{})
	wantCode(t, err, codes.InvalidArgument)
}

func TestCheckInRegistersAgent(t *testing.T) {
	e := setup(t)
	resp, err := e.agent.CheckIn(ctxT(t), &mdmv1.CheckInRequest{
		ClientId: "a1", Status: mdmv1.ClientStatus_CLIENT_STATUS_IDLE,
		Tags: []string{"lab"}, Description: "test box",
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.GetStartSession() || resp.GetSessionId() != "" {
		t.Fatalf("unexpected start instruction: %v", resp)
	}

	clients := e.srv.Clients()
	if len(clients) != 1 || clients[0].ID != "a1" || clients[0].Description != "test box" ||
		len(clients[0].Tags) != 1 || clients[0].Tags[0] != "lab" {
		t.Fatalf("Clients() = %+v", clients)
	}
}

func TestSessionLifecycle(t *testing.T) {
	e := setup(t)
	ctx := ctxT(t)

	adm := attach(t, e, "a1")
	wantEvent(t, adm, mdmv1.SessionState_SESSION_STATE_WAITING)

	// The agent must be told to start exactly once.
	var sessionID string
	testutil.WaitFor(t, "start instruction", func() bool {
		resp, err := e.agent.CheckIn(ctx, &mdmv1.CheckInRequest{ClientId: "a1"})
		if err != nil {
			t.Fatal(err)
		}
		sessionID = resp.GetSessionId()
		return resp.GetStartSession()
	})
	if sessionID == "" {
		t.Fatal("empty session id")
	}
	resp, _ := e.agent.CheckIn(ctx, &mdmv1.CheckInRequest{ClientId: "a1"})
	if resp.GetStartSession() {
		t.Fatal("start instruction delivered twice")
	}

	// Agent attaches.
	ag, err := e.agent.ProxySession(ctx)
	if err != nil {
		t.Fatal(err)
	}
	err = ag.Send(&mdmv1.ProxyFrame{Payload: &mdmv1.ProxyFrame_Hello{
		Hello: &mdmv1.ProxyHello{ClientId: "a1", SessionId: sessionID},
	}})
	if err != nil {
		t.Fatal(err)
	}
	wantEvent(t, adm, mdmv1.SessionState_SESSION_STATE_CONNECTED)

	// ListAgents reports the session.
	list, err := e.admin.ListAgents(ctx, &mdmv1.ListAgentsRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(list.GetAgents()) != 1 || !list.GetAgents()[0].GetInSession() {
		t.Fatalf("ListAgents = %v", list)
	}

	// A second admin is rejected while the agent is busy.
	adm2 := attach(t, e, "a1")
	wantEvent(t, adm2, mdmv1.SessionState_SESSION_STATE_REJECTED)

	// admin -> agent
	if err := adm.Send(&mdmv1.AdminFrame{Payload: &mdmv1.AdminFrame_Data{Data: []byte("ping")}}); err != nil {
		t.Fatal(err)
	}
	f, err := ag.Recv()
	if err != nil || string(f.GetData()) != "ping" {
		t.Fatalf("agent got %v, %v", f, err)
	}

	// agent -> admin
	if err := ag.Send(&mdmv1.ProxyFrame{Payload: &mdmv1.ProxyFrame_Data{Data: []byte("pong")}}); err != nil {
		t.Fatal(err)
	}
	af, err := adm.Recv()
	if err != nil || string(af.GetData()) != "pong" {
		t.Fatalf("admin got %v, %v", af, err)
	}

	// Agent disconnect ends the session and marks it offline.
	if _, err := e.agent.Disconnect(ctx, &mdmv1.DisconnectRequest{ClientId: "a1"}); err != nil {
		t.Fatal(err)
	}
	ev := wantEvent(t, adm, mdmv1.SessionState_SESSION_STATE_ENDED)
	if ev.GetMessage() != "agent disconnected" {
		t.Errorf("reason = %q", ev.GetMessage())
	}
	if c := e.srv.Clients(); c[0].Status != mdmv1.ClientStatus_CLIENT_STATUS_OFFLINE {
		t.Errorf("status = %v", c[0].Status)
	}

	// The claim is released, so a new admin can attach again.
	testutil.WaitFor(t, "claim release", func() bool {
		l, _ := e.admin.ListAgents(ctx, &mdmv1.ListAgentsRequest{})
		return !l.GetAgents()[0].GetInSession()
	})
}

func TestAdminDisconnectReleasesClaim(t *testing.T) {
	e := setup(t)
	ctx, cancel := context.WithCancel(ctxT(t))
	st, err := e.admin.Session(ctx)
	if err != nil {
		t.Fatal(err)
	}
	_ = st.Send(&mdmv1.AdminFrame{Payload: &mdmv1.AdminFrame_Attach{Attach: &mdmv1.AdminAttach{AgentId: "a1"}}})
	wantEvent(t, st, mdmv1.SessionState_SESSION_STATE_WAITING)
	_, _ = e.agent.CheckIn(ctxT(t), &mdmv1.CheckInRequest{ClientId: "a1"})

	cancel()
	testutil.WaitFor(t, "claim release", func() bool {
		l, err := e.admin.ListAgents(ctxT(t), &mdmv1.ListAgentsRequest{})
		return err == nil && len(l.GetAgents()) == 1 && !l.GetAgents()[0].GetInSession()
	})

	// A fresh admin can now claim the agent.
	adm := attach(t, e, "a1")
	wantEvent(t, adm, mdmv1.SessionState_SESSION_STATE_WAITING)
}

func TestSessionRequiresAttach(t *testing.T) {
	e := setup(t)
	st, _ := e.admin.Session(ctxT(t))
	_ = st.Send(&mdmv1.AdminFrame{Payload: &mdmv1.AdminFrame_Data{Data: []byte("x")}})
	_, err := st.Recv()
	wantCode(t, err, codes.InvalidArgument)

	st, _ = e.admin.Session(ctxT(t))
	_ = st.Send(&mdmv1.AdminFrame{Payload: &mdmv1.AdminFrame_Attach{Attach: &mdmv1.AdminAttach{}}})
	_, err = st.Recv()
	wantCode(t, err, codes.InvalidArgument)
}

func TestProxySessionValidation(t *testing.T) {
	e := setup(t)

	hello := func(client, session string) *mdmv1.ProxyFrame {
		return &mdmv1.ProxyFrame{Payload: &mdmv1.ProxyFrame_Hello{
			Hello: &mdmv1.ProxyHello{ClientId: client, SessionId: session},
		}}
	}
	cases := map[string]struct {
		frame *mdmv1.ProxyFrame
		code  codes.Code
	}{
		"not a hello":         {&mdmv1.ProxyFrame{Payload: &mdmv1.ProxyFrame_Data{Data: []byte("x")}}, codes.InvalidArgument},
		"hello without sess":  {hello("a1", ""), codes.InvalidArgument},
		"no matching session": {hello("a1", "nope"), codes.FailedPrecondition},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			st, err := e.agent.ProxySession(ctxT(t))
			if err != nil {
				t.Fatal(err)
			}
			_ = st.Send(tc.frame)
			_, err = st.Recv()
			wantCode(t, err, tc.code)
		})
	}

	t.Run("wrong session id for live agent", func(t *testing.T) {
		adm := attach(t, e, "a2")
		wantEvent(t, adm, mdmv1.SessionState_SESSION_STATE_WAITING)
		st, _ := e.agent.ProxySession(ctxT(t))
		_ = st.Send(hello("a2", "wrong"))
		_, err := st.Recv()
		wantCode(t, err, codes.FailedPrecondition)
	})
}

func TestDisconnect(t *testing.T) {
	e := setup(t)
	_, err := e.agent.Disconnect(ctxT(t), &mdmv1.DisconnectRequest{})
	wantCode(t, err, codes.InvalidArgument)

	// Unknown agents are accepted and not registered.
	if _, err := e.agent.Disconnect(ctxT(t), &mdmv1.DisconnectRequest{ClientId: "ghost"}); err != nil {
		t.Fatal(err)
	}
	if len(e.srv.Clients()) != 0 {
		t.Fatal("ghost agent registered")
	}
}

func TestListAgentsEmpty(t *testing.T) {
	e := setup(t)
	resp, err := e.admin.ListAgents(ctxT(t), &mdmv1.ListAgentsRequest{})
	if err != nil || len(resp.GetAgents()) != 0 {
		t.Fatalf("resp=%v err=%v", resp, err)
	}
}

// failStore fails the selected operations to exercise error mapping.
type failStore struct {
	store.Store
	failCheckIn, failList, failGet, failClaim, failOffline bool
}

var errBackend = errors.New("backend down")

func (f *failStore) RecordCheckIn(ctx context.Context, c store.ClientInfo) (bool, error) {
	if f.failCheckIn {
		return false, errBackend
	}
	return f.Store.RecordCheckIn(ctx, c)
}

func (f *failStore) ListClients(ctx context.Context) ([]store.ClientInfo, error) {
	if f.failList {
		return nil, errBackend
	}
	return f.Store.ListClients(ctx)
}

func (f *failStore) GetSession(ctx context.Context, id string) (store.SessionMeta, bool, error) {
	if f.failGet {
		return store.SessionMeta{}, false, errBackend
	}
	return f.Store.GetSession(ctx, id)
}

func (f *failStore) ClaimSession(ctx context.Context, m store.SessionMeta, ttl time.Duration) (bool, error) {
	if f.failClaim {
		return false, errBackend
	}
	return f.Store.ClaimSession(ctx, m, ttl)
}

func (f *failStore) MarkOffline(ctx context.Context, id string) error {
	if f.failOffline {
		return errBackend
	}
	return f.Store.MarkOffline(ctx, id)
}

func TestStoreErrorsMapToUnavailable(t *testing.T) {
	fs := &failStore{Store: memory.New()}
	e := setup(t, server.WithStore(fs))
	ctx := ctxT(t)
	req := &mdmv1.CheckInRequest{ClientId: "a1"}

	fs.failCheckIn = true
	_, err := e.agent.CheckIn(ctx, req)
	wantCode(t, err, codes.Unavailable)
	fs.failCheckIn = false

	fs.failGet = true
	_, err = e.agent.CheckIn(ctx, req)
	wantCode(t, err, codes.Unavailable)
	fs.failGet = false

	fs.failOffline = true
	_, err = e.agent.Disconnect(ctx, &mdmv1.DisconnectRequest{ClientId: "a1"})
	wantCode(t, err, codes.Unavailable)
	fs.failOffline = false

	fs.failList = true
	_, err = e.admin.ListAgents(ctx, &mdmv1.ListAgentsRequest{})
	wantCode(t, err, codes.Unavailable)
	if e.srv.Clients() != nil {
		t.Error("Clients() should be nil on store error")
	}
	fs.failList = false

	// ListAgents also fails when a per-agent session lookup fails.
	fs.failGet = true
	_, err = e.admin.ListAgents(ctx, &mdmv1.ListAgentsRequest{})
	wantCode(t, err, codes.Unavailable)
	fs.failGet = false

	fs.failClaim = true
	st := attach(t, e, "a1")
	_, err = st.Recv()
	wantCode(t, err, codes.Unavailable)
}

func TestWithStoreUsesProvidedStore(t *testing.T) {
	st := memory.New()
	e := setup(t, server.WithStore(st))
	_, _ = e.agent.CheckIn(ctxT(t), &mdmv1.CheckInRequest{ClientId: "a1"})
	l, _ := st.ListClients(context.Background())
	if len(l) != 1 {
		t.Fatalf("custom store has %d clients, want 1", len(l))
	}
}
