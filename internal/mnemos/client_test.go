package mnemos

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestIngest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ingest" {
			t.Errorf("path = %s, want /ingest", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		var req IngestRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatalf("decode body: %v", err)
		}
		if req.Text != "test content" {
			t.Errorf("text = %q, want %q", req.Text, "test content")
		}
		if req.Metadata["run_id"] != "run_123" {
			t.Errorf("metadata.run_id = %q, want %q", req.Metadata["run_id"], "run_123")
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(IngestResponse{Status: "accepted", QueueID: 1})
	}))
	defer server.Close()

	c := NewClient(server.URL, WithTimeout(5*time.Second))
	if err := c.Ingest(context.Background(), "test content", map[string]string{"run_id": "run_123"}); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
}

func TestHealth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/health" {
			t.Errorf("path = %s, want /health", r.URL.Path)
		}
		if r.Method != http.MethodGet {
			t.Errorf("method = %s, want GET", r.Method)
		}
		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(map[string]string{"status": "ok"})
	}))
	defer server.Close()

	c := NewClient(server.URL)
	if err := c.Health(context.Background()); err != nil {
		t.Fatalf("Health: %v", err)
	}
}

func TestIngestError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	c := NewClient(server.URL)
	if err := c.Ingest(context.Background(), "test", nil); err == nil {
		t.Fatal("expected error, got nil")
	}
}

func TestIngestAPIKey(t *testing.T) {
	var gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(IngestResponse{Status: "accepted", QueueID: 1})
	}))
	defer server.Close()

	c := NewClient(server.URL, WithAPIKey("secret-key"))
	if err := c.Ingest(context.Background(), "test", nil); err != nil {
		t.Fatalf("Ingest: %v", err)
	}
	if gotAuth != "Bearer secret-key" {
		t.Errorf("Authorization = %q, want %q", gotAuth, "Bearer secret-key")
	}
}
