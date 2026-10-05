// Package admin implements the admin tool: it attaches to a target agent via
// the server, waits until the proxy session is live, then relays terminal
// input to the agent and prints what comes back.
package admin

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"text/tabwriter"
	"time"

	"google.golang.org/grpc"

	mdmv1 "github.com/Cyb3r-Jak3/go-grpc-proxy/gen/mdm/v1"
	"github.com/Cyb3r-Jak3/go-grpc-proxy/transport"
)

// Config configures the admin tool.
type Config struct {
	ServerAddr string
	AgentID    string
	Log        *slog.Logger
	// In defaults to os.Stdin when nil.
	In io.Reader
	// Out defaults to os.Stdout when nil.
	Out io.Writer
	// DialOptions overrides the default gRPC dial options (insecure transport
	// credentials). Supply grpc.WithTransportCredentials for TLS.
	DialOptions []grpc.DialOption
}

// Run connects to the server, requests a session with the target agent, and
// blocks until the session ends or ctx is cancelled.
func Run(ctx context.Context, cfg Config) error {
	if cfg.Log == nil {
		cfg.Log = slog.Default()
	}
	if cfg.In == nil {
		cfg.In = os.Stdin
	}
	if cfg.Out == nil {
		cfg.Out = os.Stdout
	}
	if cfg.AgentID == "" {
		return errors.New("agent id is required")
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	conn, err := grpc.NewClient(cfg.ServerAddr, cfg.dialOptions()...)
	if err != nil {
		return err
	}
	defer conn.Close()

	stream, err := mdmv1.NewAdminServiceClient(conn).Session(ctx)
	if err != nil {
		return err
	}

	// First frame: attach to the target agent.
	if err := stream.Send(&mdmv1.AdminFrame{
		Payload: &mdmv1.AdminFrame_Attach{
			Attach: &mdmv1.AdminAttach{AgentId: cfg.AgentID},
		},
	}); err != nil {
		return err
	}

	fmt.Fprintf(cfg.Out, "Requesting session with agent %q...\n", cfg.AgentID)

	connected := make(chan struct{})
	recvErr := make(chan error, 1)

	// Receive loop: handle lifecycle events and agent output.
	go func() {
		var connectedOnce bool
		for {
			frame, err := stream.Recv()
			if err != nil {
				recvErr <- err
				return
			}

			if ev := frame.GetEvent(); ev != nil {
				switch ev.GetState() {
				case mdmv1.SessionState_SESSION_STATE_WAITING:
					fmt.Fprintf(cfg.Out, "[server] waiting: %s\n", ev.GetMessage())
				case mdmv1.SessionState_SESSION_STATE_CONNECTED:
					fmt.Fprintf(cfg.Out, "[server] connected: %s\n", ev.GetMessage())
					if !connectedOnce {
						connectedOnce = true
						close(connected)
					}
				case mdmv1.SessionState_SESSION_STATE_REJECTED:
					fmt.Fprintf(cfg.Out, "[server] rejected: %s\n", ev.GetMessage())
					recvErr <- errRejected
					return
				case mdmv1.SessionState_SESSION_STATE_ENDED:
					fmt.Fprintf(cfg.Out, "[server] session ended: %s\n", ev.GetMessage())
					recvErr <- io.EOF
					return
				}
				continue
			}

			if d := frame.GetData(); d != nil {
				fmt.Fprintf(cfg.Out, "agent> %s\n", string(d))
			}
		}
	}()

	// Block until the session is live, the stream fails, or we're cancelled.
	select {
	case <-connected:
	case err := <-recvErr:
		return normalizeErr(err)
	case <-ctx.Done():
		return nil
	}

	fmt.Fprintln(cfg.Out, "Session established. Type text and press Enter to send. Ctrl-C to quit.")

	// Read terminal input and send it to the agent. Run in a goroutine so a
	// server-side close can unblock us via ctx cancellation.
	go func() {
		scanner := bufio.NewScanner(cfg.In)
		for scanner.Scan() {
			if err := stream.Send(&mdmv1.AdminFrame{
				Payload: &mdmv1.AdminFrame_Data{Data: []byte(scanner.Text())},
			}); err != nil {
				cfg.Log.Warn("send failed", "error", err)
				cancel()
				return
			}
		}
		if err := scanner.Err(); err != nil {
			cfg.Log.Warn("read failed", "error", err)
		}
		// Stdin closed (EOF): end the session.
		_ = stream.CloseSend()
		cancel()
	}()

	select {
	case err := <-recvErr:
		return normalizeErr(err)
	case <-ctx.Done():
		return nil
	}
}

// ListAgents prints the server's current agent roster and returns.
func ListAgents(ctx context.Context, cfg Config) error {
	if cfg.Out == nil {
		cfg.Out = os.Stdout
	}

	conn, err := grpc.NewClient(cfg.ServerAddr, cfg.dialOptions()...)
	if err != nil {
		return err
	}
	defer conn.Close()

	resp, err := mdmv1.NewAdminServiceClient(conn).ListAgents(ctx, &mdmv1.ListAgentsRequest{})
	if err != nil {
		return err
	}

	agents := resp.GetAgents()
	if len(agents) == 0 {
		fmt.Fprintln(cfg.Out, "No agents known to the server.")
		return nil
	}

	w := tabwriter.NewWriter(cfg.Out, 0, 2, 2, ' ', 0)
	fmt.Fprintln(w, "CLIENT ID\tSTATUS\tIN SESSION\tTAGS\tDESCRIPTION\tLAST CHECK-IN")
	for _, a := range agents {
		inSession := "no"
		if a.GetInSession() {
			inSession = "yes"
		}
		tags := strings.Join(a.GetTags(), ",")
		if tags == "" {
			tags = "-"
		}
		description := a.GetDescription()
		if description == "" {
			description = "-"
		}
		last := a.GetLastCheckIn().AsTime()
		fmt.Fprintf(w, "%s\t%s\t%s\t%s\t%s\t%s (%s ago)\n",
			a.GetClientId(),
			strings.TrimPrefix(a.GetStatus().String(), "CLIENT_STATUS_"),
			inSession,
			tags,
			description,
			last.Format(time.RFC3339),
			time.Since(last).Truncate(time.Second),
		)
	}
	return w.Flush()
}

var errRejected = errors.New("session rejected by server")

func normalizeErr(err error) error {
	if errors.Is(err, io.EOF) {
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
