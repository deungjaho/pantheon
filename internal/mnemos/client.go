// Package mnemos provides an HTTP client for the Mnemos memory service.
// The client calls Mnemos's HTTP API — it does NOT import Mnemos's code.
// This keeps Pantheon decoupled from Mnemos's internals; the only contract
// is the /ingest, /query, /stats, /health shape.
package mnemos

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"
)

// DefaultTimeout is the default upper bound for a single Mnemos HTTP request.
const DefaultTimeout = 10 * time.Second

// Client calls Mnemos's HTTP API for memory ingestion and health checks.
type Client struct {
	baseURL string
	apiKey  string
	client  *http.Client
}

// Option configures the Client.
type Option func(*Client)

// WithAPIKey sets the bearer API key.
func WithAPIKey(key string) Option {
	return func(c *Client) { c.apiKey = key }
}

// WithTimeout sets the HTTP timeout.
func WithTimeout(d time.Duration) Option {
	return func(c *Client) { c.client.Timeout = d }
}

// NewClient creates a new Mnemos client.
func NewClient(baseURL string, opts ...Option) *Client {
	c := &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		client:  &http.Client{Timeout: DefaultTimeout},
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// IngestRequest is the body for POST /ingest.
type IngestRequest struct {
	Text     string            `json:"text"`
	Metadata map[string]string `json:"metadata,omitempty"`
}

// IngestResponse is the response from POST /ingest.
type IngestResponse struct {
	Status  string `json:"status"`
	QueueID int    `json:"queue_id"`
}

// Ingest sends content to Mnemos for ingestion. Ingest is asynchronous on
// the Mnemos side (returns a queue_id); a nil error means the content was
// accepted into the ingest queue, not that embedding is complete.
func (c *Client) Ingest(ctx context.Context, text string, metadata map[string]string) error {
	body, err := json.Marshal(IngestRequest{Text: text, Metadata: metadata})
	if err != nil {
		return fmt.Errorf("mnemos: marshal ingest request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.baseURL+"/ingest", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("mnemos: new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("mnemos: ingest request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("mnemos: ingest: unexpected status %d", resp.StatusCode)
	}
	return nil
}

// Health checks if Mnemos is reachable (GET /health returns HTTP 200).
func (c *Client) Health(ctx context.Context) error {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, c.baseURL+"/health", nil)
	if err != nil {
		return fmt.Errorf("mnemos: new request: %w", err)
	}
	resp, err := c.client.Do(req)
	if err != nil {
		return fmt.Errorf("mnemos: health request: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("mnemos: health: unexpected status %d", resp.StatusCode)
	}
	return nil
}
