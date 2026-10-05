// Package transport centralizes gRPC transport configuration so security can
// be changed in exactly one place.
//
// The scaffold runs with insecure credentials for easy localhost development.
// To enable TLS (recommended for any real MDM deployment), replace the
// insecure.NewCredentials() calls below with credentials.NewTLS(cfg) / a
// credentials.TransportCredentials built from your CA + certs. Nothing else in
// the server, client, or admin needs to change.
package transport

import (
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// DialOption returns the transport credentials dial option used by the client
// and admin tools.
func DialOption() grpc.DialOption {
	return grpc.WithTransportCredentials(insecure.NewCredentials())
}

// ServerOption returns the transport credentials server option used by the
// server.
func ServerOption() grpc.ServerOption {
	return grpc.Creds(insecure.NewCredentials())
}
