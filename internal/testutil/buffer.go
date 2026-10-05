package testutil

import (
	"bytes"
	"strings"
	"sync"
	"testing"
	"time"
)

// SyncBuffer is a bytes.Buffer safe for concurrent use.
type SyncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *SyncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *SyncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// WaitFor polls cond until it returns true or the timeout elapses.
func WaitFor(t testing.TB, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// WaitForOutput waits until b contains substr.
func WaitForOutput(t testing.TB, b *SyncBuffer, substr string) {
	t.Helper()
	WaitFor(t, "output "+substr, func() bool { return strings.Contains(b.String(), substr) })
}
