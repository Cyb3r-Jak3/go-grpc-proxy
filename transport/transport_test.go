package transport

import "testing"

func TestOptionsNotNil(t *testing.T) {
	if DialOption() == nil {
		t.Error("DialOption() is nil")
	}
	if ServerOption() == nil {
		t.Error("ServerOption() is nil")
	}
}
