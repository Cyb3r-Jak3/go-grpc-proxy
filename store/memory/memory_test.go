package memory

import (
	"context"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mdmv1 "github.com/Cyb3r-Jak3/go-grpc-proxy/gen/mdm/v1"
	"github.com/Cyb3r-Jak3/go-grpc-proxy/store"
)

var ctx = context.Background()

func TestRecordCheckIn(t *testing.T) {
	s := New()
	before := time.Now()
	isNew, err := s.RecordCheckIn(ctx, store.ClientInfo{
		ID: "a", Status: mdmv1.ClientStatus_CLIENT_STATUS_IDLE,
		Tags: []string{"x"}, Description: "d",
		LastCheckIn: time.Unix(1, 0), // must be ignored
	})
	if err != nil || !isNew {
		t.Fatalf("first check-in: isNew=%v err=%v", isNew, err)
	}
	isNew, _ = s.RecordCheckIn(ctx, store.ClientInfo{ID: "a", Status: mdmv1.ClientStatus_CLIENT_STATUS_BUSY})
	if isNew {
		t.Fatal("second check-in reported new")
	}

	list, _ := s.ListClients(ctx)
	if len(list) != 1 {
		t.Fatalf("len = %d, want 1", len(list))
	}
	got := list[0]
	if got.Status != mdmv1.ClientStatus_CLIENT_STATUS_BUSY {
		t.Errorf("status = %v", got.Status)
	}
	if got.LastCheckIn.Before(before) {
		t.Errorf("LastCheckIn %v not stamped by store", got.LastCheckIn)
	}
	if len(got.Tags) != 0 || got.Description != "" {
		t.Errorf("tags/description should be overwritten, got %v %q", got.Tags, got.Description)
	}
}

func TestRecordCheckInClonesTags(t *testing.T) {
	s := New()
	tags := []string{"a"}
	_, _ = s.RecordCheckIn(ctx, store.ClientInfo{ID: "a", Tags: tags})
	tags[0] = "mutated"
	list, _ := s.ListClients(ctx)
	if list[0].Tags[0] != "a" {
		t.Fatal("store aliases caller's tags slice")
	}
}

func TestListClientsReturnsCopies(t *testing.T) {
	s := New()
	_, _ = s.RecordCheckIn(ctx, store.ClientInfo{ID: "a", Tags: []string{"t"}})
	list, _ := s.ListClients(ctx)
	list[0].Tags[0] = "mutated"
	list[0].Status = mdmv1.ClientStatus_CLIENT_STATUS_BUSY
	again, _ := s.ListClients(ctx)
	if again[0].Tags[0] != "t" || again[0].Status == mdmv1.ClientStatus_CLIENT_STATUS_BUSY {
		t.Fatal("ListClients exposes internal state")
	}
}

func TestListClientsContents(t *testing.T) {
	s := New()
	for _, id := range []string{"a", "b", "c"} {
		_, _ = s.RecordCheckIn(ctx, store.ClientInfo{ID: id})
	}
	l, _ := s.ListClients(ctx)
	var got []string
	for _, c := range l {
		got = append(got, c.ID)
	}
	slices.Sort(got)
	if !slices.Equal(got, []string{"a", "b", "c"}) {
		t.Fatalf("got %v", got)
	}
}

func TestMarkOffline(t *testing.T) {
	s := New()
	if err := s.MarkOffline(ctx, "ghost"); err != nil {
		t.Fatal(err)
	}
	if l, _ := s.ListClients(ctx); len(l) != 0 {
		t.Fatal("unknown agent was created")
	}
	_, _ = s.RecordCheckIn(ctx, store.ClientInfo{ID: "a", Status: mdmv1.ClientStatus_CLIENT_STATUS_IDLE})
	_ = s.MarkOffline(ctx, "a")
	l, _ := s.ListClients(ctx)
	if l[0].Status != mdmv1.ClientStatus_CLIENT_STATUS_OFFLINE {
		t.Fatalf("status = %v", l[0].Status)
	}
}

func TestClaimSession(t *testing.T) {
	s := New()
	ok, err := s.ClaimSession(ctx, store.SessionMeta{ID: "s1", AgentID: "a"}, 0)
	if !ok || err != nil {
		t.Fatalf("ok=%v err=%v", ok, err)
	}
	ok, err = s.ClaimSession(ctx, store.SessionMeta{ID: "s2", AgentID: "a"}, 0)
	if ok || err != nil {
		t.Fatalf("second claim: ok=%v err=%v, want false,nil", ok, err)
	}
	if ok, _ = s.ClaimSession(ctx, store.SessionMeta{ID: "s3", AgentID: "b"}, 0); !ok {
		t.Fatal("different agent should be claimable")
	}
	m, found, _ := s.GetSession(ctx, "a")
	if !found || m.ID != "s1" {
		t.Fatalf("GetSession = %+v %v", m, found)
	}
	if _, found, _ := s.GetSession(ctx, "none"); found {
		t.Fatal("unexpected session")
	}
}

func TestClaimTTLExpires(t *testing.T) {
	s := New()
	_, _ = s.ClaimSession(ctx, store.SessionMeta{ID: "s1", AgentID: "a"}, 10*time.Millisecond)
	time.Sleep(30 * time.Millisecond)
	if _, found, _ := s.GetSession(ctx, "a"); found {
		t.Fatal("claim should have expired")
	}
	if ok, _ := s.ClaimSession(ctx, store.SessionMeta{ID: "s2", AgentID: "a"}, 0); !ok {
		t.Fatal("expired claim should be replaceable")
	}
	time.Sleep(30 * time.Millisecond)
	if _, found, _ := s.GetSession(ctx, "a"); !found {
		t.Fatal("zero TTL must not expire")
	}
}

func TestMarkStartDelivered(t *testing.T) {
	s := New()
	_, _ = s.ClaimSession(ctx, store.SessionMeta{ID: "s1", AgentID: "a"}, 0)

	if first, _ := s.MarkStartDelivered(ctx, "a", "other"); first {
		t.Error("wrong session ID should return false")
	}
	if first, _ := s.MarkStartDelivered(ctx, "none", "s1"); first {
		t.Error("missing session should return false")
	}
	if first, _ := s.MarkStartDelivered(ctx, "a", "s1"); !first {
		t.Error("first call should return true")
	}
	if first, _ := s.MarkStartDelivered(ctx, "a", "s1"); first {
		t.Error("second call should return false")
	}
	if m, _, _ := s.GetSession(ctx, "a"); !m.StartDelivered {
		t.Error("StartDelivered not persisted")
	}
}

func TestMarkStartDeliveredAtomic(t *testing.T) {
	s := New()
	_, _ = s.ClaimSession(ctx, store.SessionMeta{ID: "s1", AgentID: "a"}, 0)
	var wins atomic.Int32
	var wg sync.WaitGroup
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if first, _ := s.MarkStartDelivered(ctx, "a", "s1"); first {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("%d winners, want 1", wins.Load())
	}
}

func TestClaimSessionAtomic(t *testing.T) {
	s := New()
	var wins atomic.Int32
	var wg sync.WaitGroup
	for i := range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			m := store.SessionMeta{ID: fmt.Sprint(i), AgentID: "a"}
			if ok, _ := s.ClaimSession(ctx, m, 0); ok {
				wins.Add(1)
			}
		}()
	}
	wg.Wait()
	if wins.Load() != 1 {
		t.Fatalf("%d winners, want 1", wins.Load())
	}
}

func TestReleaseSession(t *testing.T) {
	s := New()
	_, _ = s.ClaimSession(ctx, store.SessionMeta{ID: "s1", AgentID: "a"}, 0)

	_ = s.ReleaseSession(ctx, "a", "stale")
	if _, found, _ := s.GetSession(ctx, "a"); !found {
		t.Fatal("release with wrong ID removed the session")
	}
	_ = s.ReleaseSession(ctx, "a", "s1")
	if _, found, _ := s.GetSession(ctx, "a"); found {
		t.Fatal("session not released")
	}
	if err := s.ReleaseSession(ctx, "a", "s1"); err != nil {
		t.Fatalf("releasing missing session: %v", err)
	}
	if ok, _ := s.ClaimSession(ctx, store.SessionMeta{ID: "s2", AgentID: "a"}, 0); !ok {
		t.Fatal("agent should be claimable after release")
	}
}
