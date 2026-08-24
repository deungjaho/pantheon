package hydra

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestListModels(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/models" {
			t.Errorf("path = %q, want %q", r.URL.Path, "/v1/models")
		}
		if r.Method != http.MethodGet {
			t.Errorf("method = %q, want %q", r.Method, http.MethodGet)
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"object":"list","data":[
		  {"id":"gpt-4","object":"model","owned_by":"api_provider"},
		  {"id":"claude-3","object":"model","owned_by":"antigravity"}
		]}`)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "")
	models, err := c.ListModels(context.Background())
	if err != nil {
		t.Fatalf("ListModels: unexpected error: %v", err)
	}
	if len(models) != 2 {
		t.Fatalf("expected 2 models, got %d", len(models))
	}
	if models[0].ID != "gpt-4" {
		t.Errorf("models[0].ID = %q, want %q", models[0].ID, "gpt-4")
	}
	if models[0].Object != "model" {
		t.Errorf("models[0].Object = %q, want %q", models[0].Object, "model")
	}
	if models[0].OwnedBy != "api_provider" {
		t.Errorf("models[0].OwnedBy = %q, want %q", models[0].OwnedBy, "api_provider")
	}
	if models[1].ID != "claude-3" {
		t.Errorf("models[1].ID = %q, want %q", models[1].ID, "claude-3")
	}
}

func TestListModels_Empty(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"object":"list","data":[]}`)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "")
	models, err := c.ListModels(context.Background())
	if err != nil {
		t.Fatalf("ListModels: unexpected error: %v", err)
	}
	if models == nil {
		t.Fatal("expected non-nil empty slice, got nil")
	}
	if len(models) != 0 {
		t.Fatalf("expected 0 models, got %d", len(models))
	}
}

func TestListModels_APIKeySent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer secret-key" {
			t.Errorf("Authorization = %q, want %q", got, "Bearer secret-key")
		}
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{"object":"list","data":[]}`)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "secret-key")
	if _, err := c.ListModels(context.Background()); err != nil {
		t.Fatalf("ListModels: unexpected error: %v", err)
	}
}

func TestListModels_Unauthorized(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		fmt.Fprint(w, `{"error":"invalid api key"}`)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "bad-key")
	_, err := c.ListModels(context.Background())
	if err == nil {
		t.Fatal("expected error for 401, got nil")
	}
}

func TestListModels_MalformedJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		fmt.Fprint(w, `{not valid json}`)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "")
	_, err := c.ListModels(context.Background())
	if err == nil {
		t.Fatal("expected error for malformed JSON, got nil")
	}
}

func TestHealthz_Healthy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/healthz" {
			t.Errorf("path = %q, want %q", r.URL.Path, "/healthz")
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "")
	if err := c.Healthz(context.Background()); err != nil {
		t.Fatalf("Healthz: unexpected error: %v", err)
	}
}

func TestHealthz_Unhealthy(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		fmt.Fprint(w, "starting up")
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "")
	if err := c.Healthz(context.Background()); err == nil {
		t.Fatal("expected error for 503, got nil")
	}
}

func TestConnectionRefused(t *testing.T) {
	// Use a port that is almost certainly not listening.
	c := NewClient("http://127.0.0.1:1", "", WithTimeout(2*time.Second))
	_, err := c.ListModels(context.Background())
	if err == nil {
		t.Fatal("expected error for connection refused, got nil")
	}
	if err := c.Healthz(context.Background()); err == nil {
		t.Fatal("expected error for connection refused, got nil")
	}
}

func TestTimeout(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "ok")
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "", WithTimeout(100*time.Millisecond))
	_, err := c.ListModels(context.Background())
	if err == nil {
		t.Fatal("expected timeout error, got nil")
	}
}

func TestContextCancelled(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(2 * time.Second)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	c := NewClient(srv.URL, "")
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, err := c.ListModels(ctx)
	if err == nil {
		t.Fatal("expected context-deadline error, got nil")
	}
}

func TestEmptyBaseURL(t *testing.T) {
	c := NewClient("", "")
	_, err := c.ListModels(context.Background())
	if err == nil {
		t.Fatal("expected error for empty base URL, got nil")
	}
	if err := c.Healthz(context.Background()); err == nil {
		t.Fatal("expected error for empty base URL, got nil")
	}
}

func TestNewClient_Defaults(t *testing.T) {
	c := NewClient("http://localhost:8080", "key")
	if c.baseURL != "http://localhost:8080" {
		t.Errorf("baseURL = %q, want %q", c.baseURL, "http://localhost:8080")
	}
	if c.apiKey != "key" {
		t.Errorf("apiKey = %q, want %q", c.apiKey, "key")
	}
	if c.httpClient == nil {
		t.Fatal("expected non-nil httpClient")
	}
	if c.httpClient.Timeout != DefaultTimeout {
		t.Errorf("timeout = %v, want %v", c.httpClient.Timeout, DefaultTimeout)
	}
}

func TestNewClient_TrimsTrailingSlash(t *testing.T) {
	c := NewClient("http://localhost:8080/", "")
	if c.baseURL != "http://localhost:8080" {
		t.Errorf("baseURL = %q, want %q (trailing slash trimmed)", c.baseURL, "http://localhost:8080")
	}
}
