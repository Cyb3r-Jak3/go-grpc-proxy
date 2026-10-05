package ids

import (
	"regexp"
	"testing"
)

func TestNewFormat(t *testing.T) {
	re := regexp.MustCompile(`^[0-9a-f]{16}$`)
	if id := New(); !re.MatchString(id) {
		t.Fatalf("New() = %q, want 16 hex chars", id)
	}
}

func TestNewUnique(t *testing.T) {
	seen := make(map[string]struct{})
	for range 1000 {
		id := New()
		if _, dup := seen[id]; dup {
			t.Fatalf("duplicate id %q", id)
		}
		seen[id] = struct{}{}
	}
}
