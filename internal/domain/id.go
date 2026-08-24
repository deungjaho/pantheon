package domain

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
)

// ErrIDGeneration is returned when NewID cannot read cryptographic randomness.
var ErrIDGeneration = errors.New("domain: id generation failed")

// NewID returns a prefixed, 128-bit, crypto/rand-backed opaque identifier.
// Format: <prefix><32 hex chars>, e.g. "run_8f3c1a...". Sortability is not
// required: SQLite event seq and timestamp provide ordering. Uniqueness comes
// from 128 bits of cryptographic randomness, not from a process counter, so
// IDs are safe across restarts, processes, and hosts.
//
// Returns an error wrapping ErrIDGeneration if the system CSPRNG is
// unavailable. Callers must handle the error explicitly; this package is not
// a process boundary and must not panic.
func NewID(prefix string) (string, error) {
	if prefix != "" && !strings.HasSuffix(prefix, "_") {
		prefix += "_"
	}
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		return "", fmt.Errorf("%w: %v", ErrIDGeneration, err)
	}
	return prefix + hex.EncodeToString(b[:]), nil
}
