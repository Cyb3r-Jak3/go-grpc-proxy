// Command server runs the MDM proxy server.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"sort"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	mdmv1 "github.com/Cyb3r-Jak3/go-grpc-proxy/gen/mdm/v1"
	"github.com/Cyb3r-Jak3/go-grpc-proxy/internal/version"
	"github.com/Cyb3r-Jak3/go-grpc-proxy/server"
	"github.com/Cyb3r-Jak3/go-grpc-proxy/transport"
	"github.com/urfave/cli/v3"
)

func main() {
	app := &cli.Command{
		Name:    "server",
		Version: version.VersionString,
		Usage:   "run the MDM proxy server",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "addr", Value: "localhost:50051", Usage: "address to listen on"},
			&cli.DurationFlag{Name: "status-interval", Value: 0 * time.Second, Usage: "how often to log the agent roster; 0 disables logging"},
			&cli.BoolFlag{
				Name: "debug",
				Usage: "Enable debug mode. " +
					"Can also be set using the DEBUG environment variable.",
				Sources: cli.EnvVars("DEBUG"),
			},
		},
		Action: runServer,
	}
	sort.Sort(cli.FlagsByName(app.Flags))
	err := app.Run(context.Background(), os.Args)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runServer(ctx context.Context, c *cli.Command) error {
	addr := c.String("addr")
	statusEvery := c.Duration("status-interval")

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: slog.LevelInfo,
	}))
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Error("listen failed", "addr", addr, "error", err)
		return err
	}

	srv := server.New(log)
	grpcServer := grpc.NewServer(transport.ServerOption())
	mdmv1.RegisterAgentServiceServer(grpcServer, srv)
	mdmv1.RegisterAdminServiceServer(grpcServer, srv)
	if c.Bool("debug") {
		log.Info("Debug mode enabled. Enabling gRPC reflection.")

		reflection.Register(grpcServer)
	}

	if statusEvery > 0 {
		go reportStatus(ctx, srv, log, statusEvery)
	}

	go func() {
		<-ctx.Done()
		log.Info("shutting down")
		grpcServer.GracefulStop()
	}()

	log.Info("server listening", "addr", addr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Error("serve failed", "error", err)
		return err
	}
	return nil
}

func reportStatus(ctx context.Context, srv *server.Server, log *slog.Logger, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			clients := srv.Clients()
			log.Info("agent roster", "count", len(clients))
			for _, c := range clients {
				log.Info("agent",
					"client_id", c.ID,
					"status", c.Status.String(),
					"last_checkin", c.LastCheckIn.Format(time.RFC3339),
					"age", time.Since(c.LastCheckIn).Truncate(time.Second).String(),
				)
			}
		}
	}
}
