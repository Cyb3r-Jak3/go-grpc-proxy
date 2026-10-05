// Package ids generates short random identifiers for agents and sessions.
package ids

import (
	"crypto/rand"
	"encoding/hex"
)

// New returns a random 16-character hex identifier.
func New() string {
	var b [8]byte
	if _, err := rand.Read(b[:]); err != nil {
		// crypto/rand should never fail; panic is appropriate if it does.
		panic("ids: cannot read random bytes: " + err.Error())
	}
	return hex.EncodeToString(b[:])
}
