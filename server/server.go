// Package server implements the MDM proxy server: an agent-facing service
// (check-in + proxy stream) and an admin-facing service, plus the registry and
// session-bridging logic that ties them together.
package server

import (
	"context"
	"log/slog"
	"sync"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	mdmv1 "github.com/Cyb3r-Jak3/go-grpc-proxy/gen/mdm/v1"
	"github.com/Cyb3r-Jak3/go-grpc-proxy/internal/ids"
	"github.com/Cyb3r-Jak3/go-grpc-proxy/store"
	"github.com/Cyb3r-Jak3/go-grpc-proxy/store/memory"
)

// relayBuffer is the number of data chunks buffered in each direction of a
// bridged session before back-pressure kicks in.
const relayBuffer = 64

// session is the process-local half of an admin<->agent bridge: the relay
// channels for the live streams. The shareable half (ID, start-delivered flag,
// the per-agent claim) lives in the store.
type session struct {
	id      string
	agentID string

	toAgent chan []byte // admin input -> agent
	toAdmin chan []byte // agent output -> admin

	connected   chan struct{} // closed when the agent's ProxySession attaches
	connectOnce sync.Once

	done      chan struct{} // closed when the session ends
	closeOnce sync.Once
	reason    string
}

func (s *session) markConnected() {
	s.connectOnce.Do(func() { close(s.connected) })
}

func (s *session) close(reason string) {
	s.closeOnce.Do(func() {
		s.reason = reason
		close(s.done)
	})
}

// Server implements both AgentServiceServer and AdminServiceServer.
type Server struct {
	mdmv1.UnimplementedAgentServiceServer
	mdmv1.UnimplementedAdminServiceServer

	log   *slog.Logger
	store store.Store

	mu   sync.Mutex
	live map[string]*session // by agent ID; streams bridged by this process
}

// Option configures a Server.
type Option func(*Server)

// WithStore sets the backend for the agent registry and session claims. If
// unset, an in-memory store is used.
func WithStore(st store.Store) Option {
	return func(s *Server) { s.store = st }
}

// New constructs a Server.
func New(log *slog.Logger, opts ...Option) *Server {
	if log == nil {
		log = slog.Default()
	}
	s := &Server{
		log:  log,
		live: make(map[string]*session),
	}
	for _, o := range opts {
		o(s)
	}
	if s.store == nil {
		s.store = memory.New()
	}
	return s
}

// Clients returns a snapshot of currently known agents, for status reporting.
func (s *Server) Clients() []store.ClientInfo {
	out, err := s.store.ListClients(context.Background())
	if err != nil {
		s.log.Error("list clients failed", "error", err)
		return nil
	}
	return out
}

func storeErr(err error) error {
	return status.Errorf(codes.Unavailable, "store: %v", err)
}

// CheckIn records an agent heartbeat and, if an admin is waiting for this
// agent, instructs it to open a proxy session.
func (s *Server) CheckIn(ctx context.Context, req *mdmv1.CheckInRequest) (*mdmv1.CheckInResponse, error) {
	clientID := req.GetClientId()
	if clientID == "" {
		return nil, status.Error(codes.InvalidArgument, "client_id is required")
	}

	isNew, err := s.store.RecordCheckIn(ctx, store.ClientInfo{
		ID:          clientID,
		Status:      req.GetStatus(),
		Tags:        req.GetTags(),
		Description: req.GetDescription(),
	})
	if err != nil {
		return nil, storeErr(err)
	}
	if isNew {
		s.log.Info("new agent registered", "client_id", clientID)
	}

	resp := &mdmv1.CheckInResponse{}
	meta, waiting, err := s.store.GetSession(ctx, clientID)
	if err != nil {
		return nil, storeErr(err)
	}
	if waiting && !meta.StartDelivered {
		first, err := s.store.MarkStartDelivered(ctx, clientID, meta.ID)
		if err != nil {
			return nil, storeErr(err)
		}
		if first {
			resp.StartSession = true
			resp.SessionId = meta.ID
			s.log.Info("instructing agent to open proxy session",
				"client_id", clientID, "session_id", meta.ID)
		}
	}
	return resp, nil
}

// Disconnect records that an agent is shutting down: it marks the agent offline
// and tears down any session so the admin side ends cleanly.
func (s *Server) Disconnect(ctx context.Context, req *mdmv1.DisconnectRequest) (*mdmv1.DisconnectResponse, error) {
	if req.GetClientId() == "" {
		return nil, status.Error(codes.InvalidArgument, "client_id is required")
	}

	if err := s.store.MarkOffline(ctx, req.GetClientId()); err != nil {
		return nil, storeErr(err)
	}

	s.mu.Lock()
	sess := s.live[req.GetClientId()]
	s.mu.Unlock()

	if sess != nil {
		// The Session / ProxySession handlers' deferred cleanup removes the
		// session from the map once their streams unwind.
		sess.close("agent disconnected")
	}

	s.log.Info("agent disconnected", "client_id", req.GetClientId())
	return &mdmv1.DisconnectResponse{}, nil
}

// ProxySession is the agent-initiated bidirectional stream. The server matches
// it to a waiting session and bridges it to the admin.
func (s *Server) ProxySession(stream mdmv1.AgentService_ProxySessionServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	hello := first.GetHello()
	if hello == nil || hello.GetSessionId() == "" {
		return status.Error(codes.InvalidArgument, "first proxy frame must be a hello with session_id")
	}

	s.mu.Lock()
	sess, ok := s.live[hello.GetClientId()]
	s.mu.Unlock()
	if !ok || sess.id != hello.GetSessionId() {
		return status.Error(codes.FailedPrecondition, "no matching session for this agent")
	}

	s.log.Info("agent proxy session connected",
		"client_id", hello.GetClientId(), "session_id", sess.id)
	sess.markConnected()
	defer sess.close("agent disconnected")

	// agent -> admin
	go func() {
		for {
			f, err := stream.Recv()
			if err != nil {
				sess.close("agent stream closed")
				return
			}
			if d := f.GetData(); d != nil {
				select {
				case sess.toAdmin <- d:
				case <-sess.done:
					return
				}
			}
		}
	}()

	// admin -> agent
	for {
		select {
		case d := <-sess.toAgent:
			if err := stream.Send(&mdmv1.ProxyFrame{Payload: &mdmv1.ProxyFrame_Data{Data: d}}); err != nil {
				sess.close("agent send failed")
				return err
			}
		case <-sess.done:
			return nil
		}
	}
}

// Session is the admin-initiated bidirectional stream. The admin attaches to a
// target agent; the server waits for that agent to check in and open its proxy
// stream, then bridges the two.
func (s *Server) Session(stream mdmv1.AdminService_SessionServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	attach := first.GetAttach()
	if attach == nil || attach.GetAgentId() == "" {
		return status.Error(codes.InvalidArgument, "first admin frame must be an attach with agent_id")
	}
	agentID := attach.GetAgentId()

	sess := &session{
		id:        ids.New(),
		agentID:   agentID,
		toAgent:   make(chan []byte, relayBuffer),
		toAdmin:   make(chan []byte, relayBuffer),
		connected: make(chan struct{}),
		done:      make(chan struct{}),
	}

	// Register the local bridge before claiming in the store, so the agent's
	// ProxySession can never arrive to find a claim without a bridge.
	s.mu.Lock()
	if _, busy := s.live[agentID]; busy {
		s.mu.Unlock()
		return s.rejectBusy(stream, agentID)
	}
	s.live[agentID] = sess
	s.mu.Unlock()

	unregister := func() {
		s.mu.Lock()
		if s.live[agentID] == sess {
			delete(s.live, agentID)
		}
		s.mu.Unlock()
	}

	ok, err := s.store.ClaimSession(stream.Context(),
		store.SessionMeta{ID: sess.id, AgentID: agentID}, 0)
	if err != nil {
		unregister()
		return storeErr(err)
	}
	if !ok {
		unregister()
		return s.rejectBusy(stream, agentID)
	}

	s.log.Info("admin requested session", "agent_id", agentID, "session_id", sess.id)

	defer func() {
		unregister()
		// The stream context is cancelled by now; release must still happen.
		if err := s.store.ReleaseSession(context.WithoutCancel(stream.Context()), agentID, sess.id); err != nil {
			s.log.Error("release session failed", "agent_id", agentID, "session_id", sess.id, "error", err)
		}
		sess.close("admin disconnected")
		s.log.Info("admin session ended", "agent_id", agentID, "session_id", sess.id)
	}()

	if err := stream.Send(sessionEvent(mdmv1.SessionState_SESSION_STATE_WAITING,
		"waiting for agent to check in")); err != nil {
		return err
	}

	// admin -> session relay
	go func() {
		for {
			f, err := stream.Recv()
			if err != nil {
				sess.close("admin stream closed")
				return
			}
			if d := f.GetData(); d != nil {
				select {
				case sess.toAgent <- d:
				case <-sess.done:
					return
				}
			}
		}
	}()

	select {
	case <-sess.connected:
		if err := stream.Send(sessionEvent(mdmv1.SessionState_SESSION_STATE_CONNECTED,
			"agent connected")); err != nil {
			return err
		}
	case <-sess.done:
		return stream.Send(sessionEvent(mdmv1.SessionState_SESSION_STATE_ENDED, sess.reason))
	case <-stream.Context().Done():
		return stream.Context().Err()
	}

	// session -> admin relay
	for {
		select {
		case d := <-sess.toAdmin:
			if err := stream.Send(&mdmv1.AdminFrame{Payload: &mdmv1.AdminFrame_Data{Data: d}}); err != nil {
				sess.close("admin send failed")
				return err
			}
		case <-sess.done:
			return stream.Send(sessionEvent(mdmv1.SessionState_SESSION_STATE_ENDED, sess.reason))
		case <-stream.Context().Done():
			return stream.Context().Err()
		}
	}
}

// ListAgents returns every known agent and whether it currently has a session.
func (s *Server) ListAgents(ctx context.Context, _ *mdmv1.ListAgentsRequest) (*mdmv1.ListAgentsResponse, error) {
	clients, err := s.store.ListClients(ctx)
	if err != nil {
		return nil, storeErr(err)
	}

	resp := &mdmv1.ListAgentsResponse{Agents: make([]*mdmv1.AgentInfo, 0, len(clients))}
	for _, c := range clients {
		_, inSession, err := s.store.GetSession(ctx, c.ID)
		if err != nil {
			return nil, storeErr(err)
		}
		resp.Agents = append(resp.Agents, &mdmv1.AgentInfo{
			ClientId:    c.ID,
			Status:      c.Status,
			LastCheckIn: timestamppb.New(c.LastCheckIn),
			InSession:   inSession,
			Tags:        c.Tags,
			Description: c.Description,
		})
	}
	return resp, nil
}

func (s *Server) rejectBusy(stream mdmv1.AdminService_SessionServer, agentID string) error {
	s.log.Info("rejecting admin: agent busy", "agent_id", agentID)
	return stream.Send(sessionEvent(mdmv1.SessionState_SESSION_STATE_REJECTED,
		"agent already has an active session"))
}

func sessionEvent(state mdmv1.SessionState, msg string) *mdmv1.AdminFrame {
	return &mdmv1.AdminFrame{
		Payload: &mdmv1.AdminFrame_Event{
			Event: &mdmv1.SessionEvent{State: state, Message: msg},
		},
	}
}
