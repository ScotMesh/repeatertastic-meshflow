package meshflow

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client posts to one meshflow-api server as one feeder (managed node).
type Client struct {
	base    string // scheme://host[:port][/prefix], no trailing /api
	key     string
	feeder  uint32
	http    *http.Client
	version string
}

// NewClient checks the API URL. apiURL may end in /api (as meshflow-bot docs sometimes show).
func NewClient(apiURL, key string, feeder uint32, botVersion string) (*Client, error) {
	base, err := BaseURL(apiURL)
	if err != nil {
		return nil, err
	}
	return &Client{base: base, key: key, feeder: feeder, version: botVersion, http: &http.Client{Timeout: 30 * time.Second}}, nil
}

// BaseURL normalises the API URL setting.
func BaseURL(apiURL string) (string, error) {
	u, err := url.Parse(strings.TrimSpace(apiURL))
	if err != nil || (u.Scheme != "https" && u.Scheme != "http") || u.Host == "" {
		return "", errors.New("the Meshflow API URL must look like https://meshflow.example.org")
	}
	u.RawQuery, u.Fragment = "", ""
	u.Path = strings.TrimSuffix(strings.TrimSuffix(u.Path, "/"), "/api")
	return strings.TrimSuffix(u.String(), "/"), nil
}

// HTTPError is a response meshflow-api refused.
type HTTPError struct {
	Status int
	Body   string
}

func (e *HTTPError) Error() string {
	body := strings.TrimSpace(e.Body)
	if len(body) > 300 {
		body = body[:300] + "…"
	}
	return fmt.Sprintf("meshflow-api %d: %s", e.Status, body)
}

// Retryable: a network error, rate limit or server error; worth trying again later.
func Retryable(err error) bool {
	var he *HTTPError
	if errors.As(err, &he) {
		return he.Status == http.StatusTooManyRequests || he.Status >= 500
	}
	return err != nil && !errors.Is(err, context.Canceled)
}

func (c *Client) feederPath(suffix string) string {
	return fmt.Sprintf("%s/api/v3/packets/%d/%s", c.base, c.feeder, suffix)
}

// Ingest uploads one packet (see PacketJSON).
func (c *Client) Ingest(ctx context.Context, body []byte) error {
	return c.do(ctx, http.MethodPost, c.feederPath("ingest/"), body)
}

// UpsertNode uploads one node (see NodeJSON).
func (c *Client) UpsertNode(ctx context.Context, body []byte) error {
	return c.do(ctx, http.MethodPost, c.feederPath("nodes/"), body)
}

// ReportVersion tells the server which bot feeds it.
func (c *Client) ReportVersion(ctx context.Context) error {
	v := c.version
	if len(v) > 128 {
		v = v[:128]
	}
	return c.do(ctx, http.MethodPut, c.feederPath("bot-version/"), []byte(fmt.Sprintf(`{"bot_version":%q}`, v)))
}

func (c *Client) do(ctx context.Context, method, url string, body []byte) error {
	req, err := http.NewRequestWithContext(ctx, method, url, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Authorization", "Token "+c.key)
	req.Header.Set("User-Agent", c.version)
	resp, err := c.http.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	// 304: meshflow-api already has it (or it was encrypted); not an error.
	if resp.StatusCode < 300 || resp.StatusCode == http.StatusNotModified {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
		return nil
	}
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	return &HTTPError{Status: resp.StatusCode, Body: string(b)}
}
