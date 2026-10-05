# go-grpc-proxy

A three-part gRPC proxy for a Mobile Device Management (MDM) platform, intended
as a reusable Go library. It has three components that speak protobuf to each
other:

- **server** — listens for agents and admins, tracks each agent's status and
  last check-in, and bridges an admin to an agent on demand.
- **client** (agent) — generates a random client ID at startup, checks in every
  X seconds, and opens a proxy session when the server asks. For now it simply
  echoes back whatever text it receives.
- **admin** — targets an agent by ID, asks the server to start a session on that
  agent's next check-in, waits until the bridge is live, then relays terminal
  input to the agent and prints the echoes.

## How a session works

```
 admin                     server                     agent (client)
   |  Session(attach=AGENT) -> |                          |
   |  <- WAITING               |                          |
   |                           |  <- CheckIn(AGENT)        |  (every X seconds)
   |                           |  CheckIn{start_session} ->|
   |                           |  <- ProxySession(hello)   |
   |  <- CONNECTED             |  (server bridges streams) |
   |  data "hello" ----------> | ----------------------->  |
   |  <- data "hello"          | <-----------------------  |  (echo)
```

- Transport: insecure localhost credentials, isolated in
  [`transport`](transport/transport.go) so TLS is a one-file
  change.
- Concurrency: **one active session per agent**. A second admin targeting a busy
  agent is rejected with `agent already has an active session`.

## Prerequisites

- Go (this module targets `go 1.27`).
- [`buf`](https://buf.build). `buf.gen.yaml` uses buf's **remote** plugins
  (`buf.build/protocolbuffers/go` and `buf.build/grpc/go`), so no local
  `protoc-gen-*` install is needed — `buf generate` runs them for you.

  If you prefer local plugins instead, install them and swap the `remote:`
  entries in `buf.gen.yaml` for `local:`:

  ```bash
  go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
  go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest
  ```

## Build

```bash
# 1. Generate protobuf/gRPC code into ./gen
task generate

# 2. Resolve dependencies
task tidy

# 3. Build everything
task build
```

## Run the demo

Three terminals:

```bash
# Terminal 1 — server
go run ./cmd/server

# Terminal 2 — agent (note the client_id it logs on startup)
go run ./cmd/client
```

```bash
# Terminal 3 — admin (use the client_id from terminal 2)
go run ./cmd/admin -agent <CLIENT_ID>
```

The admin prints `waiting...`, then `connected` once the agent's next check-in
opens the proxy stream. Type a line and press Enter; the agent echoes it back as
`agent> <your text>`. Ctrl-C in the admin ends the session.

### List known agents

Instead of copying the client ID from the agent's log, ask the server:

```bash
go run ./cmd/admin -list
```

```
CLIENT ID         STATUS   IN SESSION  LAST CHECK-IN
a1b2c3d4e5f6a7b8  IDLE     no          2026-10-04T12:00:00Z (3s ago)
```

### Graceful shutdown

When the agent receives Ctrl-C/SIGTERM it sends a best-effort `Disconnect` to the
server before exiting. The server then tears down any active or pending session
(so the admin sees a clean `session ended` rather than a stream error) and marks
the agent `OFFLINE` in the roster.

### Flags

| Component | Flag | Default | Purpose |
|-----------|------|---------|---------|
| server | `-addr` | `localhost:50051` | listen address |
| server | `-status-interval` | `15s` | how often to log the agent roster (`0` disables) |
| client | `-server` | `localhost:50051` | server address |
| client | `-interval` | `5s` | check-in interval |
| admin | `-server` | `localhost:50051` | server address |
| admin | `-agent` | *(required unless `-list`)* | target client ID |
| admin | `-list` | `false` | list known agents and exit |

## Layout

```
proto/mdm/v1/mdm.proto   protobuf schema (source of truth)
gen/mdm/v1/              generated Go (buf generate)
server/                  registry + session bridge + service impls
client/                  agent check-in loop + pluggable session handler (echo by default)
admin/                   attach + interactive relay
transport/               transport credentials (swap in TLS here)
internal/ids/            random ID generation
cmd/{server,client,admin}  thin main() entrypoints
```

## Using as a library

```sh
go get github.com/Cyb3r-Jak3/go-grpc-proxy
```

Public packages: `server`, `client`, `admin`, `transport`, `store` (+ `store/memory`)
and the generated `gen/mdm/v1`.

```go
// Embed the server in your own gRPC server.
srv := server.New(log, server.WithStore(myStore))
mdmv1.RegisterAgentServiceServer(grpcServer, srv)
mdmv1.RegisterAdminServiceServer(grpcServer, srv)

// Run an agent with TLS.
client.Run(ctx, client.Config{
    AgentID:     "my-agent",
    ServerAddr:  "mdm.example.com:443",
    DialOptions: []grpc.DialOption{grpc.WithTransportCredentials(creds)},
})
```

### Custom session behavior

By default the agent echoes back whatever the admin sends (`client.EchoHandler`).
Set `Config.SessionHandler` to change that. The handler runs once per proxy
session, after the hello handshake, and receives a `*client.Session` with
`Recv() ([]byte, error)` and `Send([]byte) error`. Return when `Recv` yields
`io.EOF` (the admin ended the session) or `ctx` is cancelled.

```go
client.Run(ctx, client.Config{
    AgentID:    "my-agent",
    ServerAddr: "mdm.example.com:443",
    SessionHandler: func(ctx context.Context, s *client.Session) error {
        for {
            in, err := s.Recv()
            if err != nil {
                return err
            }
            if err := s.Send(bytes.ToUpper(in)); err != nil {
                return err
            }
        }
    },
})
```

`Send` must not be called from multiple goroutines at once.

## Development note

This project was created using AI-assisted "vibecoding" with Claude. All code has been reviewed and approved by a human developer.
