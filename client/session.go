package client

import (
	"context"
	"log/slog"

	mdmv1 "github.com/Cyb3r-Jak3/go-grpc-proxy/gen/mdm/v1"
)

// Session is the byte stream between an agent and the admin attached to it.
// The hello handshake is already done by the time a SessionHandler receives it.
type Session struct {
	// ID is the server-assigned session ID.
	ID string
	// Log is the agent's logger.
	Log *slog.Logger

	stream mdmv1.AgentService_ProxySessionClient
}

// Recv blocks for the next chunk of data sent by the admin. It returns io.EOF
// when the admin ends the session. Non-data frames are skipped.
func (s *Session) Recv() ([]byte, error) {
	for {
		frame, err := s.stream.Recv()
		if err != nil {
			return nil, err
		}
		if data := frame.GetData(); data != nil {
			return data, nil
		}
	}
}

// Send delivers data to the admin. Like the underlying gRPC stream, Send must
// not be called concurrently from multiple goroutines.
func (s *Session) Send(data []byte) error {
	return s.stream.Send(&mdmv1.ProxyFrame{
		Payload: &mdmv1.ProxyFrame_Data{Data: data},
	})
}

// SessionHandler implements the agent's behavior for one proxy session. It
// runs until the session is over: return nil when Recv yields io.EOF or ctx is
// cancelled (Run treats an io.EOF error as a clean close too).
type SessionHandler func(ctx context.Context, s *Session) error

// EchoHandler is the default SessionHandler: it echoes every chunk back.
func EchoHandler(_ context.Context, s *Session) error {
	for {
		data, err := s.Recv()
		if err != nil {
			return err
		}
		if err := s.Send(data); err != nil {
			return err
		}
	}
}
