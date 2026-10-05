// Command client runs a managed agent that checks in and echoes proxy traffic.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/Cyb3r-Jak3/go-grpc-proxy/client"
	"github.com/Cyb3r-Jak3/go-grpc-proxy/internal/ids"
	"github.com/urfave/cli/v3"
)

func main() {
	app := &cli.Command{
		Name:  "client",
		Usage: "run a managed agent that checks in and echoes proxy traffic",
		Flags: []cli.Flag{
			&cli.StringFlag{Name: "server", Value: "localhost:50051", Usage: "server address"},
			&cli.StringFlag{Name: "id", Usage: "agent id (randomly generated if empty)"},
			&cli.DurationFlag{Name: "interval", Value: 5 * time.Second, Usage: "check-in interval"},
			&cli.StringFlag{Name: "tags", Usage: "comma-separated tags reported on check-in"},
			&cli.StringFlag{Name: "description", Usage: "description reported on check-in"},
		},
		Action: runClient,
	}
	sort.Sort(cli.FlagsByName(app.Flags))
	err := app.Run(context.Background(), os.Args)
	if err != nil {
		_, _ = fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
}

func runClient(ctx context.Context, c *cli.Command) error {
	serverAddr := c.String("server")
	clientID := c.String("id")
	interval := c.Duration("interval")
	tags := c.String("tags")
	description := c.String("description")

	if clientID == "" {
		clientID = ids.New()
	}

	var tagList []string
	if tags != "" {
		for t := range strings.SplitSeq(tags, ",") {
			if t = strings.TrimSpace(t); t != "" {
				tagList = append(tagList, t)
			}
		}
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))

	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := client.Run(ctx, client.Config{
		AgentID:     clientID,
		ServerAddr:  serverAddr,
		Interval:    interval,
		Log:         log,
		Tags:        tagList,
		Description: description,
	}); err != nil {
		log.Error("agent exited with error", "error", err)
		return err
	}
	log.Info("agent exited")
	return nil
}
