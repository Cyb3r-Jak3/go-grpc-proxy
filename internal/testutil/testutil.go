// Package testutil provides an in-memory gRPC harness for tests.
package testutil

import (
	"context"
	"net"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	mdmv1 "github.com/Cyb3r-Jak3/go-grpc-proxy/gen/mdm/v1"
)

// Service is a server implementing both the agent and admin services.
type Service interface {
	mdmv1.AgentServiceServer
	mdmv1.AdminServiceServer
}

// Start serves srv over an in-memory listener and returns dial options that
// reach it, plus a ready-made connection. Everything is cleaned up with t.
func Start(t testing.TB, srv Service) ([]grpc.DialOption, *grpc.ClientConn) {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	gs := grpc.NewServer()
	mdmv1.RegisterAgentServiceServer(gs, srv)
	mdmv1.RegisterAdminServiceServer(gs, srv)
	go func() { _ = gs.Serve(lis) }()
	t.Cleanup(gs.Stop)

	opts := []grpc.DialOption{
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return lis.Dial() }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	conn, err := grpc.NewClient("passthrough:///bufnet", opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { conn.Close() })
	return opts, conn
}
