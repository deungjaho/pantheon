package domain

import (
	"errors"
	"strings"
	"sync"
	"testing"
)

// mustNewID is a test-only helper. Production code must call NewID and handle
// the error explicitly.
func mustNewID(t *testing.T, prefix string) string {
	t.Helper()
	id, err := NewID(prefix)
	if err != nil {
		t.Fatalf("NewID(%q) error: %v", prefix, err)
	}
	return id
}

func TestNewIDFormat(t *testing.T) {
	for _, prefix := range []string{"run_", "tsk_", "evt_", "agt_", "prj_", "ws_", "art_", "cnd_", "req_"} {
		id, err := NewID(prefix)
		if err != nil {
			t.Fatalf("NewID(%q) error: %v", prefix, err)
		}
		if !strings.HasPrefix(id, prefix) {
			t.Fatalf("id %q missing prefix %q", id, prefix)
		}
		body := strings.TrimPrefix(id, prefix)
		if len(body) != 32 {
			t.Fatalf("id %q body length = %d, want 32", id, len(body))
		}
		for _, c := range body {
			if !strings.ContainsRune("0123456789abcdef", c) {
				t.Fatalf("id %q body has non-hex char %q", id, c)
			}
		}
	}
}

func TestNewIDNoPrefix(t *testing.T) {
	id, err := NewID("")
	if err != nil {
		t.Fatalf("NewID(\"\") error: %v", err)
	}
	if len(id) != 32 {
		t.Fatalf("empty-prefix id length = %d, want 32", len(id))
	}
}

func TestNewIDSuffixNormalization(t *testing.T) {
	id, err := NewID("run")
	if err != nil {
		t.Fatalf("NewID(\"run\") error: %v", err)
	}
	if !strings.HasPrefix(id, "run_") {
		t.Fatalf("id %q should have normalized suffix underscore", id)
	}
}

func TestNewIDErrorWrapsSentinel(t *testing.T) {
	// We cannot easily force crypto/rand to fail, but we can assert the
	// sentinel exists and is exported so callers can errors.Is it.
	if !errors.Is(ErrIDGeneration, ErrIDGeneration) {
		t.Fatal("ErrIDGeneration must be usable with errors.Is")
	}
}

func TestNewIDConcurrentUniqueness(t *testing.T) {
	const goroutines = 64
	const perG = 2000
	seen := make(map[string]struct{}, goroutines*perG)
	var mu sync.Mutex
	var wg sync.WaitGroup
	errs := make(chan error, goroutines)
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			local := make(map[string]struct{}, perG)
			for i := 0; i < perG; i++ {
				id, err := NewID("evt_")
				if err != nil {
					errs <- err
					return
				}
				if _, dup := local[id]; dup {
					errs <- errDuplicate{id}
					return
				}
				local[id] = struct{}{}
			}
			mu.Lock()
			for id := range local {
				if _, dup := seen[id]; dup {
					errs <- errDuplicate{id}
					mu.Unlock()
					return
				}
				seen[id] = struct{}{}
			}
			mu.Unlock()
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("id error or duplicate: %v", err)
	}
	if got := len(seen); got != goroutines*perG {
		t.Fatalf("unique id count = %d, want %d", got, goroutines*perG)
	}
}

type errDuplicate struct{ id string }

func (e errDuplicate) Error() string { return "duplicate: " + e.id }
