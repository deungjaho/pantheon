// Package hydra provides an HTTP client for the Hydra LLM gateway, which
// provides model routing. The client calls Hydra's HTTP API — it does NOT
// import Hydra's code. This keeps Pantheon decoupled from Hydra's internals;
// the only contract is the OpenAI-compatible /v1/models and /healthz shape.
package hydra

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

// Model is a single model entry from Hydra's /v1/models endpoint. The JSON
// tags mirror the OpenAI-compatible model object shape returned by Hydra.
type Model struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	OwnedBy string `json:"owned_by"`
}

// DefaultTimeout is the default upper bound for a single Hydra HTTP request.
const DefaultTimeout = 10 * time.Second

// Client calls Hydra's HTTP API for model listing and health checks.
type Client struct {
	baseURL    string // e.g., "http://localhost:8080"
	apiKey     string // optional API key (Bearer token)
	httpClient *http.Client
}

// Option configures a Client.
type Option func(*Client)

// WithHTTPClient sets the underlying *http.Client.
func WithHTTPClient(hc *http.Client) Option {
	return func(c *Client) { c.httpClient = hc }
}

// WithTimeout sets the per-request timeout used when the caller's context
// has no deadline.
func WithTimeout(d time.Duration) Option {
	return func(c *Client) {
		if c.httpClient == nil {
			c.httpClient = &http.Client{}
		}
		c.httpClient.Timeout = d
	}
}

// NewClient creates a Hydra HTTP client. baseURL is the Hydra base URL
// (e.g. "http://localhost:8080"); apiKey is optional (sent as a Bearer
// token when non-empty).
func NewClient(baseURL, apiKey string, opts ...Option) *Client {
	c := &Client{
		baseURL: strings.TrimRight(baseURL, "/"),
		apiKey:  apiKey,
		httpClient: &http.Client{
			Timeout: DefaultTimeout,
		},
	}
	for _, opt := range opts {
		opt(c)
	}
	if c.httpClient == nil {
		c.httpClient = &http.Client{Timeout: DefaultTimeout}
	}
	return c
}

// modelsResponse is the OpenAI-compatible list response from /v1/models.
type modelsResponse struct {
	Object string  `json:"object"`
	Data   []Model `json:"data"`
}

// ListModels calls GET /v1/models and returns the available models.
func (c *Client) ListModels(ctx context.Context) ([]Model, error) {
	body, err := c.do(ctx, http.MethodGet, "/v1/models", nil)
	if err != nil {
		return nil, err
	}
	var resp modelsResponse
	if err := json.Unmarshal(body, &resp); err != nil {
		return nil, fmt.Errorf("hydra: parse models response: %w", err)
	}
	if resp.Data == nil {
		resp.Data = []Model{}
	}
	return resp.Data, nil
}

// Healthz calls GET /healthz and returns nil if Hydra is healthy (responds
// with HTTP 200). A non-200 status or transport error is returned.
func (c *Client) Healthz(ctx context.Context) error {
	body, err := c.do(ctx, http.MethodGet, "/healthz", nil)
	if err != nil {
		return err
	}
	// Hydra's /healthz returns the plain text "ok". We accept any 2xx
	// body (checked by do) and do not require an exact match, so a future
	// JSON health payload still works.
	_ = body
	return nil
}

// do performs an HTTP request against the Hydra base URL. It applies the
// client's configured timeout when the caller's context has no deadline.
func (c *Client) do(ctx context.Context, method, path string, body io.Reader) ([]byte, error) {
	if c.baseURL == "" {
		return nil, fmt.Errorf("hydra: base URL not configured")
	}
	url := c.baseURL + path

	// If the caller's context has no deadline, apply our own timeout so a
	// hung Hydra cannot block forever.
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, c.httpClient.Timeout)
		defer cancel()
	}

	req, err := http.NewRequestWithContext(ctx, method, url, body)
	if err != nil {
		return nil, fmt.Errorf("hydra: build request %s %s: %w", method, path, err)
	}
	if c.apiKey != "" {
		req.Header.Set("Authorization", "Bearer "+c.apiKey)
	}
	req.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("hydra: request %s %s: %w", method, path, err)
	}
	defer resp.Body.Close()

	respBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("hydra: read response %s %s: %w", method, path, err)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("hydra: %s %s: HTTP %d: %s", method, path, resp.StatusCode, truncate(string(respBody), 256))
	}
	return respBody, nil
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
