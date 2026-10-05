// Package store defines the persistence interface the MDM server uses for its
// agent registry and per-agent session claims. Implementations live in
// subpackages (see store/memory).
//
// Only shareable state belongs here. The live gRPC stream bridge (channels,
// goroutines) is process-local and stays inside the server.
package store

import (
	"context"
	"time"

	mdmv1 "github.com/Cyb3r-Jak3/go-grpc-proxy/gen/mdm/v1"
)

// ClientInfo is the server's view of a single agent.
type ClientInfo struct {
	ID          string
	Status      mdmv1.ClientStatus
	LastCheckIn time.Time
	// Tags and Description are set by the client at run time and reported on
	// every check-in.
	Tags        []string
	Description string
}

// SessionMeta is the shareable part of an admin<->agent session. At most one
// session exists per agent at a time.
type SessionMeta struct {
	ID      string
	AgentID string
	// StartDelivered is true once a CheckInResponse has told the agent to open
	// its ProxySession stream, so the agent is only asked once.
	StartDelivered bool
}

// Store holds the agent registry and session claims. Methods that decide
// between competing callers are atomic, so implementations backed by a remote
// store must use equivalent primitives (e.g. SET NX, compare-and-delete).
type Store interface {
	// RecordCheckIn upserts an agent from a check-in, stamping LastCheckIn.
	// It reports whether the agent was previously unknown.
	RecordCheckIn(ctx context.Context, c ClientInfo) (isNew bool, err error)

	// MarkOffline sets a known agent offline. Unknown agents are ignored.
	MarkOffline(ctx context.Context, clientID string) error

	// ListClients returns a snapshot of every known agent.
	ListClients(ctx context.Context) ([]ClientInfo, error)

	// ClaimSession atomically records m as the agent's session. It returns
	// false, without error, if the agent already has one. A ttl of zero means
	// the claim never expires on its own.
	ClaimSession(ctx context.Context, m SessionMeta, ttl time.Duration) (ok bool, err error)

	// GetSession returns the agent's current session, if any.
	GetSession(ctx context.Context, agentID string) (SessionMeta, bool, error)

	// MarkStartDelivered atomically sets StartDelivered on the agent's session
	// identified by sessionID. It returns true only for the first caller, and
	// false if the session is gone, replaced, or already delivered.
	MarkStartDelivered(ctx context.Context, agentID, sessionID string) (first bool, err error)

	// ReleaseSession removes the agent's session only if it is still sessionID.
	ReleaseSession(ctx context.Context, agentID, sessionID string) error
}
