package rpc

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
)

// fakeIdempotencyStore implements IdempotencyStore for testing.
type fakeIdempotencyStore struct {
	mu       sync.Mutex
	cache    map[string]json.RawMessage
	getCalls int
	setCalls int
}

func (s *fakeIdempotencyStore) GetCachedResponse(ctx context.Context, requestID string) (json.RawMessage, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.getCalls++
	resp, ok := s.cache[requestID]
	return resp, ok, nil
}

func (s *fakeIdempotencyStore) CacheResponse(ctx context.Context, requestID string, resp json.RawMessage) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.setCalls++
	s.cache[requestID] = resp
	return nil
}

func TestRequestIdIdempotency(t *testing.T) {
	var buf bytes.Buffer
	srv := NewServer(&buf)
	store := &fakeIdempotencyStore{cache: make(map[string]json.RawMessage)}
	srv.SetIdempotencyStore(store)

	callCount := 0
	srv.Register("test.echo", func(ctx context.Context, params json.RawMessage) (any, error) {
		callCount++
		return map[string]any{"n": callCount}, nil
	})

	// First request with request_id="req-1".
	req1 := `{"jsonrpc":"2.0","id":"a","method":"test.echo","request_id":"req-1"}` + "\n"
	buf.Reset()
	srv.Serve(context.Background(), strings.NewReader(req1))

	var resp1 Response
	if err := json.Unmarshal(buf.Bytes(), &resp1); err != nil {
		t.Fatalf("unmarshal resp1: %v\nraw: %s", err, buf.String())
	}
	if callCount != 1 {
		t.Errorf("after first request, callCount=%d, want 1", callCount)
	}
	if store.setCalls != 1 {
		t.Errorf("after first request, setCalls=%d, want 1", store.setCalls)
	}

	// Second request with same request_id="req-1" — should return cached, not re-execute.
	buf.Reset()
	srv.Serve(context.Background(), strings.NewReader(req1))

	var resp2 Response
	if err := json.Unmarshal(buf.Bytes(), &resp2); err != nil {
		t.Fatalf("unmarshal resp2: %v\nraw: %s", err, buf.String())
	}
	if callCount != 1 {
		t.Errorf("after retried request, callCount=%d, want 1 (cached)", callCount)
	}
	if store.getCalls != 2 {
		t.Errorf("after retried request, getCalls=%d, want 2", store.getCalls)
	}

	// Both responses should have the same result.
	r1, _ := json.Marshal(resp1.Result)
	r2, _ := json.Marshal(resp2.Result)
	if !bytes.Equal(r1, r2) {
		t.Errorf("cached response differs: %s vs %s", r1, r2)
	}
}

func TestRequestIdDifferentIdsNotCached(t *testing.T) {
	var buf bytes.Buffer
	srv := NewServer(&buf)
	store := &fakeIdempotencyStore{cache: make(map[string]json.RawMessage)}
	srv.SetIdempotencyStore(store)

	callCount := 0
	srv.Register("test.echo", func(ctx context.Context, params json.RawMessage) (any, error) {
		callCount++
		return map[string]any{"n": callCount}, nil
	})

	// Two requests with different request_ids — both should execute.
	req1 := `{"jsonrpc":"2.0","id":"a","method":"test.echo","request_id":"req-1"}` + "\n"
	req2 := `{"jsonrpc":"2.0","id":"b","method":"test.echo","request_id":"req-2"}` + "\n"

	buf.Reset()
	srv.Serve(context.Background(), strings.NewReader(req1))
	buf.Reset()
	srv.Serve(context.Background(), strings.NewReader(req2))

	if callCount != 2 {
		t.Errorf("callCount=%d, want 2 (different request_ids)", callCount)
	}
}

func TestNoRequestIDNoCaching(t *testing.T) {
	var buf bytes.Buffer
	srv := NewServer(&buf)
	store := &fakeIdempotencyStore{cache: make(map[string]json.RawMessage)}
	srv.SetIdempotencyStore(store)

	callCount := 0
	srv.Register("test.echo", func(ctx context.Context, params json.RawMessage) (any, error) {
		callCount++
		return map[string]any{"n": callCount}, nil
	})

	// Request without request_id — should not use cache.
	req := `{"jsonrpc":"2.0","id":"a","method":"test.echo"}` + "\n"
	buf.Reset()
	srv.Serve(context.Background(), strings.NewReader(req))
	buf.Reset()
	srv.Serve(context.Background(), strings.NewReader(req))

	if callCount != 2 {
		t.Errorf("callCount=%d, want 2 (no request_id = no caching)", callCount)
	}
	if store.setCalls != 0 {
		t.Errorf("setCalls=%d, want 0 (no request_id)", store.setCalls)
	}
}

func TestNoIdempotencyStoreNoCaching(t *testing.T) {
	var buf bytes.Buffer
	srv := NewServer(&buf)
	// No SetIdempotencyStore — idempotency is nil.

	callCount := 0
	srv.Register("test.echo", func(ctx context.Context, params json.RawMessage) (any, error) {
		callCount++
		return map[string]any{"n": callCount}, nil
	})

	// Same request_id twice — should execute both times (no store).
	req := `{"jsonrpc":"2.0","id":"a","method":"test.echo","request_id":"req-1"}` + "\n"
	buf.Reset()
	srv.Serve(context.Background(), strings.NewReader(req))
	buf.Reset()
	srv.Serve(context.Background(), strings.NewReader(req))

	if callCount != 2 {
		t.Errorf("callCount=%d, want 2 (no idempotency store)", callCount)
	}
}

func TestMaxLineSizeEnforced(t *testing.T) {
	var buf bytes.Buffer
	srv := NewServer(&buf)
	srv.SetMaxLineSize(100) // very small limit

	srv.Register("test.echo", func(ctx context.Context, params json.RawMessage) (any, error) {
		return map[string]any{"ok": true}, nil
	})

	// Request larger than 100 bytes — should be rejected.
	largePayload := strings.Repeat("x", 200)
	req := fmt.Sprintf(`{"jsonrpc":"2.0","id":"a","method":"test.echo","params":{"data":"%s"}}`+"\n", largePayload)

	buf.Reset()
	srv.Serve(context.Background(), strings.NewReader(req))

	var resp Response
	if err := json.Unmarshal(buf.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v\nraw: %s", err, buf.String())
	}
	if resp.Error == nil {
		t.Fatal("expected error for oversized request, got nil")
	}
	if resp.Error.Code != "INVALID_INPUT" {
		t.Errorf("error code=%s, want INVALID_INPUT", resp.Error.Code)
	}
}

func TestMaxLineSizeAllowsValidRequest(t *testing.T) {
	var buf bytes.Buffer
	srv := NewServer(&buf)
	srv.SetMaxLineSize(MaxSSHRequestSize) // 64KB

	srv.Register("test.echo", func(ctx context.Context, params json.RawMessage) (any, error) {
		return map[string]any{"ok": true}, nil
	})

	// Normal request — should pass.
	req := `{"jsonrpc":"2.0","id":"a","method":"test.echo"}` + "\n"
	buf.Reset()
	srv.Serve(context.Background(), strings.NewReader(req))

	var resp Response
	if err := json.Unmarshal(buf.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal: %v\nraw: %s", err, buf.String())
	}
	if resp.Error != nil {
		t.Errorf("unexpected error: %v", resp.Error)
	}
}

func TestMaxSSHRequestSizeIs64KB(t *testing.T) {
	if MaxSSHRequestSize != 64*1024 {
		t.Errorf("MaxSSHRequestSize=%d, want %d", MaxSSHRequestSize, 64*1024)
	}
}
