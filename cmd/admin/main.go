// Command admin opens an interactive proxy session with a target agent.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"syscall"

	"github.com/Cyb3r-Jak3/go-grpc-proxy/admin"
	"github.com/Cyb3r-Jak3/go-grpc-proxy/internal/version"
	"github.com/urfave/cli/v3"
)

func main() {
	app := &cli.Command{
		Name:    "admin",
		Version: version.VersionString,
		Usage:   "open an interactive proxy session with a target agent",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "server", Value: "localhost:50051", Usage: "server address"},
			&cli.StringFlag{Name: "agent", Usage: "target agent (client) ID (required unless -list)"},
			&cli.BoolFlag{Name: "list", Usage: "list known agents and exit"},
		},
		Action: runAdmin,
	}
	sort.Sort(cli.FlagsByName(app.Flags))
	err := app.Run(context.Background(), os.Args)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runAdmin(ctx context.Context, c *cli.Command) error {
	serverAddr := c.String("server")
	agentID := c.String("agent")
	list := c.Bool("list")

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	cfg := admin.Config{ServerAddr: serverAddr, AgentID: agentID, Log: log}

	if list {
		return admin.ListAgents(ctx, cfg)
	}

	if agentID == "" {
		return cli.Exit("missing -agent flag (or use -list)", 2)
	}

	return admin.Run(ctx, cfg)
}
