// Package memory provides an in-process store.Store backed by maps. State is
// lost on restart and is not shared between server instances.
package memory

import (
	"context"
	"slices"
	"sync"
	"time"

	mdmv1 "github.com/Cyb3r-Jak3/go-grpc-proxy/gen/mdm/v1"
	"github.com/Cyb3r-Jak3/go-grpc-proxy/store"
)

type claim struct {
	meta    store.SessionMeta
	expires time.Time // zero means never
}

func (c claim) expired(now time.Time) bool {
	return !c.expires.IsZero() && now.After(c.expires)
}

// Store is an in-memory store.Store. The zero value is not usable; call New.
type Store struct {
	mu       sync.Mutex
	clients  map[string]*store.ClientInfo // by client ID
	sessions map[string]claim             // by agent ID
}

var _ store.Store = (*Store)(nil)

// New returns an empty in-memory store.
func New() *Store {
	return &Store{
		clients:  make(map[string]*store.ClientInfo),
		sessions: make(map[string]claim),
	}
}

// live returns the unexpired claim for agentID, dropping it if expired.
// Callers must hold s.mu.
func (s *Store) live(agentID string) (claim, bool) {
	c, ok := s.sessions[agentID]
	if ok && c.expired(time.Now()) {
		delete(s.sessions, agentID)
		return claim{}, false
	}
	return c, ok
}

func (s *Store) RecordCheckIn(_ context.Context, c store.ClientInfo) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	info, ok := s.clients[c.ID]
	if !ok {
		info = &store.ClientInfo{ID: c.ID}
		s.clients[c.ID] = info
	}
	info.Status = c.Status
	info.LastCheckIn = time.Now()
	info.Tags = slices.Clone(c.Tags)
	info.Description = c.Description
	return !ok, nil
}

func (s *Store) MarkOffline(_ context.Context, clientID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if info, ok := s.clients[clientID]; ok {
		info.Status = mdmv1.ClientStatus_CLIENT_STATUS_OFFLINE
		info.LastCheckIn = time.Now()
	}
	return nil
}

func (s *Store) ListClients(_ context.Context) ([]store.ClientInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	out := make([]store.ClientInfo, 0, len(s.clients))
	for _, c := range s.clients {
		cp := *c
		cp.Tags = slices.Clone(c.Tags)
		out = append(out, cp)
	}
	return out, nil
}

func (s *Store) ClaimSession(_ context.Context, m store.SessionMeta, ttl time.Duration) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if _, busy := s.live(m.AgentID); busy {
		return false, nil
	}
	c := claim{meta: m}
	if ttl > 0 {
		c.expires = time.Now().Add(ttl)
	}
	s.sessions[m.AgentID] = c
	return true, nil
}

func (s *Store) GetSession(_ context.Context, agentID string) (store.SessionMeta, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	c, ok := s.live(agentID)
	return c.meta, ok, nil
}

func (s *Store) MarkStartDelivered(_ context.Context, agentID, sessionID string) (bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	c, ok := s.live(agentID)
	if !ok || c.meta.ID != sessionID || c.meta.StartDelivered {
		return false, nil
	}
	c.meta.StartDelivered = true
	s.sessions[agentID] = c
	return true, nil
}

func (s *Store) ReleaseSession(_ context.Context, agentID, sessionID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if c, ok := s.sessions[agentID]; ok && c.meta.ID == sessionID {
		delete(s.sessions, agentID)
	}
	return nil
}
