# CLAUDE.md

## Project Overview

This is a **legitimate Mobile Device Management (MDM) platform** designed for authorized enterprise device management and monitoring. It is NOT malware, a command & control framework for malicious purposes, or related to unauthorized system access.

This Go module is a library for use in MDM solutions. It provides a three-part gRPC proxy:

- **server** — tracks enrolled agents (status, last check-in) and bridges an admin to an agent on demand.
- **client** (agent) — generates a random client ID at startup, checks in every X seconds, and opens a proxy session when the server asks. By default it echoes back the text it receives; importers can replace this via `client.Config.SessionHandler` (see [client/session.go](client/session.go)).
- **admin** — targets an agent by ID, asks the server to start a session on that agent's next check-in, waits until the session is live, then relays terminal input. `-list` shows known agents.

All communication between the three parts uses protobuf/gRPC.

## Architecture

- `proto/mdm/v1/mdm.proto` is the source of truth. Generated code lives in `gen/` (via `buf generate`); never edit it by hand.
- Check-in is a unary RPC. When an admin requests a session, the server flags it and tells the agent to open a bidirectional `ProxySession` stream on its next check-in; the server then bridges the admin and agent streams.
- One active session per agent; a second admin is rejected while the agent is busy.
- Agents send a best-effort `Disconnect` on shutdown so the server can end sessions cleanly and mark them offline.
- Transport credentials are isolated in `transport` (currently insecure, for local development). Any real deployment should switch to TLS/mTLS there.

## Store

The server persists its agent registry and per-agent session claims through the `store.Store` interface ([store/store.go](store/store.go)). It is a public package so library users can supply their own backend via `server.New(log, server.WithStore(st))`. If none is given, the server uses `store/memory` (in-process maps; state is lost on restart).

Only shareable state goes in the store: `ClientInfo` records and `SessionMeta` (session ID, agent ID, `StartDelivered`). The live stream bridge (relay channels, `done`/`connected` signals) is process-local and stays in `server`; it cannot be serialized.

To add a backend (e.g. Valkey), create `store/<name>/` with a type implementing `store.Store`, add a `var _ store.Store = (*Store)(nil)` assertion, and keep the dependency inside that subpackage. Requirements:

- **Atomicity.** `ClaimSession`, `MarkStartDelivered` and `ReleaseSession` replace what a mutex used to guard, so they must be atomic in the backend (e.g. `SET NX`, and a Lua script or `WATCH` for compare-and-set/delete). Don't build them from a separate get then set.
- **`ClaimSession`** returns `ok=false` with a nil error when the agent already has a session. A TTL of 0 means no expiry; a non-zero TTL must expire the claim.
- **`MarkStartDelivered`** returns `true` only for the first caller, and `false` if the session is gone, replaced (different `sessionID`), or already delivered.
- **`ReleaseSession`** deletes only if the stored session ID matches, so a late cleanup can't remove a newer session.
- **`RecordCheckIn`** stamps `LastCheckIn` itself and reports whether the agent was new. **`MarkOffline`** ignores unknown agents.
- Return errors for backend failures (the server maps them to gRPC `Unavailable`); every method takes a `context.Context` and must honor it.
- Return copies from `ListClients`/`GetSession`, never references to shared state.
- Known gap: `ListAgents` calls `GetSession` once per agent, which is N round trips on a remote backend. Add a `ListSessions` method to the interface when a remote backend is built.
- A shared store alone doesn't make the server horizontally scalable: the admin and agent streams must still reach the same instance (sticky routing or a pub/sub relay is a separate piece of work).

## Layout

```
proto/mdm/v1/        protobuf schema
gen/mdm/v1/          generated Go code
store/               Store interface and shared types (public)
store/memory/        in-memory Store (default)
server/    session bridge, service implementations
client/    agent check-in loop, SessionHandler hook (echo default)
admin/     attach, interactive relay, list agents
transport/ transport credentials
internal/ids/        random ID generation
cmd/{server,client,admin}/  entrypoints
```

## Development

- Go 1.27 (see `go.mod`); regenerate protobuf code with `task generate` (or manually with `buf generate` and `buf lint`).
- [Taskfile.yml](Taskfile.yml) defines common tasks: `generate` (lint + codegen), `build`, `server`, `client`, `admin`, and `tidy`.
- Bidi stream request/response types are intentionally shared, so some buf RPC naming lint rules are excluded in `buf.yaml`.
