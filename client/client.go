// Package client implements the managed agent: it registers a random client
// ID, checks in with the server on an interval, and — when told to — opens a
// proxy session and echoes back whatever text it receives.
package client

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync/atomic"
	"time"

	"google.golang.org/grpc"

	mdmv1 "github.com/Cyb3r-Jak3/go-grpc-proxy/gen/mdm/v1"
	"github.com/Cyb3r-Jak3/go-grpc-proxy/transport"
)

// Config configures the agent.
type Config struct {
	AgentID    string
	ServerAddr string
	Interval   time.Duration
	Log        *slog.Logger
	// Tags and Description are reported on every check-in for operator-facing
	// identification and filtering; they're set once at startup.
	Tags        []string
	Description string
	// DialOptions overrides the default gRPC dial options (insecure transport
	// credentials). Supply grpc.WithTransportCredentials for TLS.
	DialOptions []grpc.DialOption
	// SessionHandler customizes what the agent does with a proxy session.
	// Defaults to EchoHandler.
	SessionHandler SessionHandler
}

// Run starts the agent and blocks until ctx is cancelled or a fatal error
// occurs.
func Run(ctx context.Context, cfg Config) error {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.Interval <= 0 {
		cfg.Interval = 5 * time.Second
	}

	cfg.Log.Info("agent starting", "client_id", cfg.AgentID, "server", cfg.ServerAddr)

	conn, err := grpc.NewClient(cfg.ServerAddr, cfg.dialOptions()...)
	if err != nil {
		return err
	}
	defer conn.Close()

	agent := mdmv1.NewAgentServiceClient(conn)

	// Best-effort goodbye on shutdown. Deferred before conn.Close (LIFO) so the
	// connection is still open when it runs. Uses a fresh context because ctx is
	// already cancelled by the time we're tearing down.
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if _, err := agent.Disconnect(shutdownCtx, &mdmv1.DisconnectRequest{ClientId: cfg.AgentID}); err != nil {
			cfg.Log.Warn("disconnect notification failed", "error", err)
		} else {
			cfg.Log.Info("notified server of shutdown", "client_id", cfg.AgentID)
		}
	}()

	// inSession guards against opening more than one proxy session at a time.
	var inSession atomic.Bool

	ticker := time.NewTicker(cfg.Interval)
	defer ticker.Stop()

	// Check in immediately on startup, then on every tick.
	for {
		status := mdmv1.ClientStatus_CLIENT_STATUS_IDLE
		if inSession.Load() {
			status = mdmv1.ClientStatus_CLIENT_STATUS_BUSY
		}

		resp, err := agent.CheckIn(ctx, &mdmv1.CheckInRequest{
			ClientId:    cfg.AgentID,
			Status:      status,
			Tags:        cfg.Tags,
			Description: cfg.Description,
		})
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			cfg.Log.Warn("check-in failed", "error", err)
		} else if resp.GetStartSession() && !inSession.Load() {
			inSession.Store(true)
			sessionID := resp.GetSessionId()
			go func() {
				defer inSession.Store(false)
				if err := runProxySession(ctx, agent, cfg, sessionID); err != nil {
					cfg.Log.Warn("proxy session ended with error", "session_id", sessionID, "error", err)
				}
			}()
		}

		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// runProxySession opens the bidirectional proxy stream, performs the hello
// handshake and hands the stream to the configured SessionHandler.
func runProxySession(
	ctx context.Context,
	agent mdmv1.AgentServiceClient,
	cfg Config,
	sessionID string,
) error {
	cfg.Log.Info("opening proxy session", "session_id", sessionID)

	stream, err := agent.ProxySession(ctx)
	if err != nil {
		return err
	}

	// Identify ourselves so the server can match us to the waiting admin.
	if err := stream.Send(&mdmv1.ProxyFrame{
		Payload: &mdmv1.ProxyFrame_Hello{
			Hello: &mdmv1.ProxyHello{ClientId: cfg.AgentID, SessionId: sessionID},
		},
	}); err != nil {
		return err
	}

	handler := cfg.SessionHandler
	if handler == nil {
		handler = EchoHandler
	}
	err = handler(ctx, &Session{ID: sessionID, Log: cfg.Log, stream: stream})
	_ = stream.CloseSend()
	if errors.Is(err, io.EOF) || ctx.Err() != nil {
		cfg.Log.Info("proxy session closed", "session_id", sessionID)
		return nil
	}
	return err
}

func (c Config) dialOptions() []grpc.DialOption {
	if len(c.DialOptions) > 0 {
		return c.DialOptions
	}
	return []grpc.DialOption{transport.DialOption()}
}
